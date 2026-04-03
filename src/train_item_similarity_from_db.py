import pandas as pd
from sqlalchemy import create_engine
from sklearn.metrics.pairwise import cosine_similarity
import os
from dotenv import load_dotenv

load_dotenv()

def train_item_based_model():
    print("--- 🧠 KHỞI ĐỘNG VÒNG LẶP AI ---")
    
    # 1. Kết nối "Ống dẫn" Database
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL không tồn tại trong file .env")
    engine = create_engine(db_url)
    
    # 2. Lấy nguyên liệu (Lịch sử tương tác)
    print("📥 1. Đang lấy dữ liệu hành vi người dùng...")
    query = """
        SELECT user_id, product_id, interaction_type
        FROM interactions
        WHERE interaction_type IN ('view', 'like', 'add_to_cart', 'purchase')
    """
    df = pd.read_sql(query, engine)

    if df.empty:
        print("⚠️ Chưa có tương tác nào. Hãy lên Web click xem và mua vài đôi giày trước nhé!")
        return

    df["user_id"] = df["user_id"].astype(str)
    df["product_id"] = df["product_id"].astype(str)
    df["interaction_type"] = df["interaction_type"].astype(str).str.strip()
    print("📊 Tổng số interactions dùng để train:", len(df))
    print("📊 Phân bố interaction type:")
    print(df["interaction_type"].value_counts())
    print("📊 Số user:", df["user_id"].nunique())
    print("📊 Số product:", df["product_id"].nunique())

    # 3. Làm sạch và Chấm điểm (Scoring)
    print("⚖️ 2. Đang chấm điểm: View (1), Cart (3), purchase (5)...")
    scores = {
        'view': 1,
        'like': 3,
        'add_to_cart': 5,
        'purchase': 8
    }
    df['score'] = df['interaction_type'].map(scores).fillna(0)
    df = df[df['score'] > 0].copy()
    
    # Nếu 1 người click xem 1 đôi giày 5 lần, ta cộng dồn điểm lại cho chính xác mức độ quan tâm
    df_grouped = df.groupby(['user_id', 'product_id'])['score'].sum().reset_index()

    # 4. Xây dựng Ma trận User-Item
    print("🧮 3. Đang xây dựng Ma trận Vector...")
    user_item_matrix = df_grouped.pivot(index='user_id', columns='product_id', values='score').fillna(0)
    print(f"📐 Kích thước user-item matrix: {user_item_matrix.shape}")

    # 5. Dùng AI tính độ tương đồng (Cosine Similarity)
    print("🤖 4. AI đang học quy luật tương quan giữa các đôi giày...")
    # Chuyển vị ma trận (T) để tính toán giữa các Đôi giày với nhau thay vì giữa các User
    item_user_matrix = user_item_matrix.T
    similarity_matrix = cosine_similarity(item_user_matrix)
    
    # Gắn lại tên ID giày vào ma trận cho dễ đọc
    item_sim_df = pd.DataFrame(similarity_matrix, index=item_user_matrix.index, columns=item_user_matrix.index)

    # 6. Lọc lấy Top 5 gợi ý tốt nhất cho mỗi sản phẩm
    print("🏆 5. Đang chọn lọc Top 5 gợi ý xuất sắc nhất...")
    recommendations = []
    
    for product_id in item_sim_df.index:
        # Sắp xếp điểm tương đồng từ cao xuống thấp
        similar_scores = item_sim_df[product_id].sort_values(ascending=False)
        
        # Bỏ qua chính đôi giày đó (vì giống chính nó luôn là 100%)
        similar_scores = similar_scores[similar_scores.index != product_id]
        
        # Lấy 5 chiếc có điểm cao nhất
        top_5 = similar_scores.head(5)
        
        rank = 1
        for sim_id, sim_score in top_5.items():
            if sim_score > 0:  # Chỉ gợi ý nếu thực sự có sự liên quan
                recommendations.append({
                    'product_id': product_id,
                    'similar_product_id': sim_id,
                    'rank': rank,
                    'similarity_score': round(sim_score, 4)
                })
            rank += 1

    # 7. Lưu kết quả lên tầng Gold (Bảng gold_item_similarity)
    if recommendations:
        print("📤 6. Đang lưu Bộ não mới lên tầng Gold...")
        final_df = pd.DataFrame(recommendations)
        # Ép kiểu dữ liệu về chuỗi (text) để không bị lỗi khi JOIN ở Backend
        final_df['product_id'] = final_df['product_id'].astype(str)
        final_df['similar_product_id'] = final_df['similar_product_id'].astype(str)
        
        # Đẩy vào Postgres
        final_df.to_sql('gold_item_similarity', engine, if_exists='replace', index=False)
        print(f"✅ HOÀN TẤT! Đã tạo ra {len(final_df)} cặp gợi ý thông minh.")
        print(f"📊 Số sản phẩm có item-similarity: {final_df['product_id'].nunique()}")
    else:
        print("⚠️ Dữ liệu chưa đủ để AI tìm ra quy luật. Hãy tạo thêm các kịch bản mua hàng chéo nhau!")

if __name__ == "__main__":
    train_item_based_model()