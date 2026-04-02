from fastapi import FastAPI, HTTPException, Depends
import psycopg2
import os
from dotenv import load_dotenv
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
# Lưu ý: Các thư mục src.auth bạn tự import nhé, mình giữ nguyên của bạn
from src.auth.hash_utils import hash_password, verify_password
from src.auth.schemas import UserCreate, UserResponse
from src.auth.jwt_handler import signJWT
from src.auth.jwt_bearer import JWTBearer

load_dotenv()
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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
    INSERT INTO interactions (user_id, product_id, interaction_type) -- Sửa thành interaction_type
    VALUES (%s, %s, %s)
    """, (user_id, req.product_id, req.action_type)) # req.action_type giữ nguyên vì nó lấy từ Pydantic
        conn.commit()
        return {"message": "Success"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()

@app.get("/recommendations/user", dependencies=[Depends(JWTBearer())])
def get_user_recommendations(token: dict = Depends(JWTBearer())):
    user_id = str(token.get("user_id")) # Đảm bảo ép kiểu string để khớp với AI
    conn = get_db_conn()
    cur = conn.cursor()
    
    try:
        # Lấy top 10 sản phẩm được AI gợi ý cho user này
        cur.execute("""
            SELECT p.product_id, p.name, p.price, p.image_url, g.score
            FROM gold_user_recommendations g
            JOIN products p ON g.product_id = p.product_id
            WHERE g.user_id = %s
            ORDER BY g.score DESC
            LIMIT 10
        """, (user_id,))
        
        rows = cur.fetchall()
        
        # Nếu chưa có gợi ý cá nhân hóa (người dùng mới hoàn toàn)
        if not rows:
            cur.execute("SELECT product_id, name, price, image_url FROM products ORDER BY RANDOM() LIMIT 10")
            rows = cur.fetchall()
            
        return [{"product_id": r[0], "name": r[1], "price": r[2], "image_url": r[3]} for r in rows]
    finally:
        cur.close()
        conn.close()

@app.get("/recommendations/user", dependencies=[Depends(JWTBearer())])
def get_user_personalized_recommendations(token: dict = Depends(JWTBearer())):
    user_id = token.get("user_id")
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        query = """
            SELECT p.product_id, p.name, p.price, p.image_url 
            FROM gold_user_recommendations g
            JOIN products p ON g.product_id::text = p.product_id
            WHERE g.user_id = %s
            ORDER BY g.rank ASC
            LIMIT 5
        """
        cur.execute(query, (user_id,))
        rows = cur.fetchall()
        return [{"product_id": r[0], "name": r[1], "price": r[2], "image_url": r[3]} for r in rows]
    except Exception as e:
        print(f"Lỗi gợi ý người dùng: {e}")
        return [] # Trả về mảng rỗng nếu lỗi
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

@app.get("/orders", dependencies=[Depends(JWTBearer())])
def get_orders(token: dict = Depends(JWTBearer())):
    user_id = token.get("user_id")
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT p.product_id, p.name, p.price, p.image_url
            FROM interactions i
            JOIN products p ON i.product_id = p.product_id
            WHERE i.user_id = %s AND i.interaction_type = 'buy'
            ORDER BY i.interaction_id DESC
        """, (user_id,))
        rows = cur.fetchall()
        return [{"product_id": r[0], "name": r[1], "price": r[2], "image_url": r[3]} for r in rows]
    finally:
        cur.close()
        conn.close()

@app.get("/cart", dependencies=[Depends(JWTBearer())])
def get_cart(token: dict = Depends(JWTBearer())):
    user_id = token.get("user_id")
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        # Dùng DISTINCT ON để nếu người dùng bấm thêm 2 lần cùng 1 đôi giày thì chỉ hiện 1 lần trong giỏ
        cur.execute("""
            SELECT DISTINCT ON (p.product_id) p.product_id, p.name, p.price, p.image_url, i.interaction_id
            FROM interactions i
            JOIN products p ON i.product_id::text = p.product_id::text
            WHERE i.user_id = %s AND i.interaction_type = 'add_to_cart'
            ORDER BY p.product_id, i.interaction_id DESC
        """, (user_id,))
        rows = cur.fetchall()
        
        # Sắp xếp lại để sản phẩm mới thêm nằm lên đầu
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