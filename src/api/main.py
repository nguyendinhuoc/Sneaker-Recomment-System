from fastapi import FastAPI, HTTPException, Depends
import psycopg2
import os
from dotenv import load_dotenv
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from src.auth.hash_utils import hash_password, verify_password
from src.auth.schemas import UserCreate, UserResponse
from src.auth.jwt_handler import signJWT
from src.auth.jwt_bearer import JWTBearer

load_dotenv()
app = FastAPI()

origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_conn():
    return psycopg2.connect(os.environ.get("DATABASE_URL"))

@app.get("/")
def read_root():
    return {"message": "Welcome to Sneaker Recommendation API"}

@app.post("/register", response_model=UserResponse)
def register_user(user: UserCreate):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        # 1. Tìm theo cột 'name' (Vì DB bạn đã đổi username -> name)
        cur.execute("SELECT user_id FROM users WHERE name = %s", (user.username,))
        if cur.fetchone():
            raise HTTPException(status_code=400, detail="Username already registered")
        
        # 2. Lấy mật khẩu thô (Vì bạn muốn bỏ băm để demo cho nhanh)
        raw_password = user.password 
        
        # 3. Đảm bảo các cột INSERT khớp 100% với bảng trên Neon
        # Chú ý: Dùng 'name' và 'password' thay vì 'username'/'password_hash'
        cur.execute("""
            INSERT INTO users (name, password, age, gender, created_at) 
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP) RETURNING user_id
        """, (user.username, raw_password, user.age, user.gender))
        
        user_id = cur.fetchone()[0]
        conn.commit()
        
        # Trả về kết quả khớp với UserResponse schema
        return {"user_id": user_id, "username": user.username}
        
    except Exception as e:
        # In lỗi thật sự ra Terminal để bạn nhìn thấy
        print(f"--- LỖI TẠI ĐÂY: {e} ---") 
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()

class LoginRequest(BaseModel):
    username: str
    password: str

@app.post("/login")
def login_user(req: LoginRequest):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        # Lấy mật khẩu từ database
        cur.execute("SELECT user_id, password FROM users WHERE name = %s", (req.username,))
        row = cur.fetchone()
        
        # So sánh trực tiếp, không dùng verify_password nữa
        if not row or req.password != row[1]:
            raise HTTPException(status_code=401, detail="Invalid username or password")
        
        return signJWT(row[0])
    finally:
        cur.close()
        conn.close()

class InteractRequest(BaseModel):
    product_id: str
    action_type: str

@app.post("/interact", dependencies=[Depends(JWTBearer())])
def record_interaction(req: InteractRequest, token: dict = Depends(JWTBearer())):
    user_id = token.get("user_id")
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO interactions (
                user_id,
                product_id,
                interaction_type,
                quantity,
                interaction_time
            )
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
        """, (
            user_id,
            req.product_id,
            req.action_type,
            1
        ))
        conn.commit()
        return {"message": "Success"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()

@app.get("/recommendations/{p_id}")
def get_item_recommendations(p_id: str):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        # 1. Lấy thông tin sản phẩm gốc
        cur.execute("""
            SELECT product_id, brand, category, style, type, purpose, color, material
            FROM products
            WHERE product_id::text = %s
        """, (p_id,))
        base_product = cur.fetchone()

        if not base_product:
            return []

        _, base_brand, base_category, base_style, base_type, base_purpose, base_color, base_material = base_product

        # 2. Lấy candidate từ bảng gold_item_similarity + metadata để rerank
        cur.execute("""
            SELECT
                p.product_id,
                p.name,
                p.price,
                p.image_url,
                p.brand,
                p.category,
                p.style,
                p.type,
                p.purpose,
                p.color,
                p.material,
                g.rank,
                g.similarity_score
            FROM gold_item_similarity g
            JOIN products p
              ON g.similar_product_id::text = p.product_id::text
            WHERE g.product_id::text = %s
            ORDER BY g.rank ASC
            LIMIT 20
        """, (p_id,))
        rows = cur.fetchall()

        ranked_items = []

        for r in rows:
            product_id, name, price, image_url, brand, category, style, type_, purpose, color, material, rank, similarity_score = r

            # không gợi ý lại chính nó
            if str(product_id) == str(p_id):
                continue
            
            # lọc cứng bớt item lệch ngữ cảnh
            same_category = category and base_category and category == base_category
            same_type = type_ and base_type and type_ == base_type
            same_purpose = purpose and base_purpose and purpose == base_purpose
            same_style = style and base_style and style == base_style

            # nếu không đủ gần thì bỏ qua
            if not ((same_category and same_type) or same_purpose or same_style):
                continue

            # tránh lệch men/women từ title
            base_name_text = ""
            candidate_name_text = (name or "").lower()

            if base_category and str(base_category).lower() == "men" and "women" in candidate_name_text:
                continue

            meta_bonus = 0

            if brand and base_brand and brand == base_brand:
                meta_bonus += 0.25
            if category and base_category and category == base_category:
                meta_bonus += 0.20
            if type_ and base_type and type_ == base_type:
                meta_bonus += 0.20
            if purpose and base_purpose and purpose == base_purpose:
                meta_bonus += 0.20
            if style and base_style and style == base_style:
                meta_bonus += 0.10
            if material and base_material and material == base_material:
                meta_bonus += 0.03
            if color and base_color and color == base_color:
                meta_bonus += 0.02

            final_score = float(similarity_score) + meta_bonus

            ranked_items.append({
                "product_id": product_id,
                "name": name,
                "price": price,
                "image_url": image_url,
                "brand": brand,
                "category": category,
                "style": style,
                "type": type_,
                "purpose": purpose,
                "color": color,
                "material": material,
                "rank": rank,
                "similarity_score": float(similarity_score),
                "final_score": final_score,
            })

        # 3. Sắp xếp lại theo final_score
        ranked_items.sort(key=lambda x: x["final_score"], reverse=True)

        # 4. Lấy tối đa 5 item tốt nhất
        result = [
            {
                "product_id": item["product_id"],
                "name": item["name"],
                "price": item["price"],
                "image_url": item["image_url"],
            }
            for item in ranked_items[:5]
        ]

        existing_ids = [str(item["product_id"]) for item in result]
        existing_ids.append(str(p_id))

        # 5. Nếu chưa đủ 5 thì fallback theo metadata gần nhất
        if len(result) < 5:
            limit_needed = 5 - len(result)

            cur.execute("""
                SELECT product_id, name, price, image_url
                FROM products
                WHERE product_id::text != ALL(%s)
                  AND (
                    brand = %s
                    OR category = %s
                    OR type = %s
                    OR purpose = %s
                    OR style = %s
                  )
                LIMIT %s
            """, (
                existing_ids,
                base_brand,
                base_category,
                base_type,
                base_purpose,
                base_style,
                limit_needed
            ))

            fallback_rows = cur.fetchall()

            for r in fallback_rows:
                result.append({
                    "product_id": r[0],
                    "name": r[1],
                    "price": r[2],
                    "image_url": r[3]
                })

        return result[:5]

    except Exception as e:
        print(f"Lỗi gọi gợi ý item: {e}")
        return []
    finally:
        cur.close()
        conn.close()

@app.get("/recommendations/for-you", dependencies=[Depends(JWTBearer())])
def get_recommendations_for_you(token: dict = Depends(JWTBearer())):
    user_id = str(token.get("user_id"))
    conn = get_db_conn()
    cur = conn.cursor()

    try:
        # 1. Đếm interaction
        cur.execute("""
            SELECT COUNT(*) 
            FROM interactions 
            WHERE user_id = %s
        """, (user_id,))
        interaction_count = cur.fetchone()[0]

        # 2. Nếu đủ dữ liệu → personalized
        if interaction_count >= 3:
            cur.execute("""
                SELECT p.product_id, p.name, p.price, p.image_url, g.score
                FROM gold_user_recommendations g
                JOIN products p ON g.product_id::text = p.product_id
                WHERE g.user_id = %s
                ORDER BY g.score DESC
                LIMIT 10
            """, (user_id,))
            
            rows = cur.fetchall()

            if rows:
                return {
                    "type": "personalized",
                    "items": [
                        {
                            "product_id": r[0],
                            "name": r[1],
                            "price": r[2],
                            "image_url": r[3],
                            "score": r[4]
                        }
                        for r in rows
                    ]
                }

        # 3. Fallback: lấy 1 sản phẩm user đã xem gần nhất
        cur.execute("""
            SELECT product_id
            FROM interactions
            WHERE user_id = %s
            ORDER BY interaction_id DESC
            LIMIT 1
        """, (user_id,))
        
        last_product = cur.fetchone()

        if last_product:
            product_id = last_product[0]

            cur.execute("""
                SELECT p.product_id, p.name, p.price, p.image_url
                FROM gold_item_similarity g
                JOIN products p ON g.similar_product_id::text = p.product_id
                WHERE g.product_id::text = %s
                ORDER BY g.rank ASC
                LIMIT 10
            """, (product_id,))

            rows = cur.fetchall()

            return {
                "type": "fallback_item_similarity",
                "items": [
                    {
                        "product_id": r[0],
                        "name": r[1],
                        "price": r[2],
                        "image_url": r[3]
                    }
                    for r in rows
                ]
            }

        # 4. fallback cuối cùng
        return {
            "type": "empty",
            "items": []
        }

    finally:
        cur.close()
        conn.close()

# CHÚ Ý ĐOẠN NÀY: PHÂN TRANG CHUẨN XÁC
@app.get("/products")
def get_products(page: int = 1, size: int = 24):
    offset = (page - 1) * size
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT product_id, name, price, image_url FROM products ORDER BY product_id::int LIMIT %s OFFSET %s", (size, offset))
        rows = cur.fetchall()
        products = [{"product_id": r[0], "name": r[1], "price": r[2], "image_url": r[3]} for r in rows]
        
        cur.execute("SELECT COUNT(*) FROM products")
        total = cur.fetchone()[0]
        
        return {
            "items": products,
            "total_pages": (total + size - 1) // size
        }
    except Exception as e:
        print(f"Lỗi lấy sản phẩm: {e}")
        return {"items": [], "total_pages": 0}
    finally:
        cur.close()
        conn.close()

@app.get("/products/detail/{p_id}")
def get_product_detail(p_id: str):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        # Lấy tất cả các cột từ bảng products (bao gồm brand, color, style...)
        cur.execute("SELECT * FROM products WHERE product_id::text = %s", (p_id,))
        row = cur.fetchone()
        
        if row:
            # Tự động map tên cột từ Database vào Dictionary
            col_names = [desc[0] for desc in cur.description]
            product_data = dict(zip(col_names, row))
            return product_data
        return {}
    except Exception as e:
        print(f"Lỗi lấy chi tiết Silver: {e}")
        return {}
    finally:
        cur.close()
        conn.close()

@app.get("/recently-viewed", dependencies=[Depends(JWTBearer())])
def get_recently_viewed(token: dict = Depends(JWTBearer())):
    user_id = token.get("user_id")
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT DISTINCT ON (p.product_id)
                p.product_id, p.name, p.price, p.image_url, i.interaction_id
            FROM interactions i
            JOIN products p ON i.product_id::text = p.product_id::text
            WHERE i.user_id = %s AND i.interaction_type = 'view'
            ORDER BY p.product_id, i.interaction_id DESC
        """, (user_id,))
        rows = cur.fetchall()

        sorted_rows = sorted(rows, key=lambda x: x[4], reverse=True)

        return [
            {
                "product_id": r[0],
                "name": r[1],
                "price": r[2],
                "image_url": r[3]
            }
            for r in sorted_rows
        ]
    except Exception as e:
        print(f"Lỗi lấy recently viewed: {e}")
        return []
    finally:
        cur.close()
        conn.close()

@app.get("/orders", dependencies=[Depends(JWTBearer())])
def get_orders(token: dict = Depends(JWTBearer())):
    user_id = token.get("user_id")
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT p.product_id, p.name, p.price, p.image_url
            FROM interactions i
            JOIN products p ON i.product_id::text = p.product_id::text
            WHERE i.user_id = %s AND i.interaction_type = 'purchase'
            ORDER BY i.interaction_id DESC
        """, (user_id,))
        rows = cur.fetchall()
        return [{"product_id": r[0], "name": r[1], "price": r[2], "image_url": r[3]} for r in rows]
    except Exception as e:
        print(f"Lỗi lấy đơn hàng: {e}")
        return []
    finally:
        cur.close()
        conn.close()

@app.get("/cart", dependencies=[Depends(JWTBearer())])
def get_cart(token: dict = Depends(JWTBearer())):
    user_id = token.get("user_id")
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT DISTINCT ON (p.product_id)
                p.product_id, p.name, p.price, p.image_url, i.interaction_id
            FROM interactions i
            JOIN products p ON i.product_id::text = p.product_id::text
            WHERE i.user_id = %s
              AND i.interaction_type = 'add_to_cart'
              AND NOT EXISTS (
                  SELECT 1
                  FROM interactions i2
                  WHERE i2.user_id = i.user_id
                    AND i2.product_id::text = i.product_id::text
                    AND i2.interaction_type = 'purchase'
              )
            ORDER BY p.product_id, i.interaction_id DESC
        """, (user_id,))
        rows = cur.fetchall()

        sorted_rows = sorted(rows, key=lambda x: x[4], reverse=True)

        return [{"product_id": r[0], "name": r[1], "price": r[2], "image_url": r[3]} for r in sorted_rows]
    except Exception as e:
        print(f"Lỗi lấy giỏ hàng: {e}")
        return []
    finally:
        cur.close()
        conn.close()

@app.get("/recommendations/{p_id}")
def get_item_recommendations(p_id: str):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        # Bước 1: Hỏi bộ não AI (Tầng Gold - Collaborative Filtering)
        cur.execute("""
            SELECT p.product_id, p.name, p.price, p.image_url 
            FROM gold_item_similarity g
            JOIN products p ON g.similar_product_id::text = p.product_id::text
            WHERE g.product_id::text = %s
            ORDER BY g.rank ASC
            LIMIT 5
        """, (p_id,))
        ai_recommendations = cur.fetchall()
        
        # Chuyển đổi kết quả AI thành list các dictionary
        result = [{"product_id": r[0], "name": r[1], "price": r[2], "image_url": r[3]} for r in ai_recommendations]
        
        # Lấy danh sách ID đã gợi ý để không bị trùng lặp
        existing_ids = [r[0] for r in ai_recommendations]
        existing_ids.append(p_id) # Tránh gợi ý lại chính đôi giày đang xem
        
        # Bước 2: NẾU AI CHƯA ĐỦ THÔNG MINH (Gợi ý < 5 đôi) -> Dùng Fallback (Tầng Silver)
        if len(result) < 5:
            # Tìm thông tin brand và category của đôi giày hiện tại
            cur.execute("SELECT brand, category FROM products WHERE product_id::text = %s", (p_id,))
            prod_info = cur.fetchone()
            
            if prod_info:
                brand, category = prod_info
                limit_needed = 5 - len(result)
                
                # Tìm các đôi giày cùng hãng (brand) hoặc cùng loại (category)
                query_fallback = """
                    SELECT product_id, name, price, image_url 
                    FROM products 
                    WHERE (brand = %s OR category = %s) 
                    AND product_id::text != ALL(%s)
                    ORDER BY RANDOM() -- Lấy ngẫu nhiên cho đa dạng
                    LIMIT %s
                """
                cur.execute(query_fallback, (brand, category, existing_ids, limit_needed))
                fallback_rows = cur.fetchall()
                
                # Bổ sung vào danh sách kết quả
                for r in fallback_rows:
                    result.append({"product_id": r[0], "name": r[1], "price": r[2], "image_url": r[3]})

        return result
    except Exception as e:
        print(f"Lỗi gọi gợi ý item: {e}")
        return []
    finally:
        cur.close()
        conn.close()

@app.get("/products/{p_id}")
def get_product_detail(p_id: str):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        # Tư duy Kỹ sư: Nếu bảng Silver của bạn có thêm cột như brand, description, category... 
        # hãy thêm nó vào câu SELECT này. Tạm thời mình lấy 4 cột cơ bản.
        cur.execute("""
            SELECT product_id, name, price, image_url 
            FROM products 
            WHERE product_id::text = %s
        """, (p_id,))
        row = cur.fetchone()
        
        if row:
            return {"product_id": row[0], "name": row[1], "price": row[2], "image_url": row[3]}
        return {} # Trả về rỗng nếu không tìm thấy
    except Exception as e:
        print(f"Lỗi lấy chi tiết sản phẩm: {e}")
        return {}
    finally:
        cur.close()
        conn.close()

@app.get("/favorites", dependencies=[Depends(JWTBearer())])
def get_favorites(token: dict = Depends(JWTBearer())):
    user_id = token.get("user_id")
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT DISTINCT ON (p.product_id)
                p.product_id, p.name, p.price, p.image_url, i.interaction_id
            FROM interactions i
            JOIN products p ON i.product_id::text = p.product_id::text
            WHERE i.user_id = %s
              AND i.interaction_type = 'like'
            ORDER BY p.product_id, i.interaction_id DESC
        """, (user_id,))
        rows = cur.fetchall()

        sorted_rows = sorted(rows, key=lambda x: x[4], reverse=True)

        return [
            {
                "product_id": r[0],
                "name": r[1],
                "price": r[2],
                "image_url": r[3]
            }
            for r in sorted_rows
        ]
    except Exception as e:
        print(f"Lỗi lấy favorites: {e}")
        return []
    finally:
        cur.close()
        conn.close()