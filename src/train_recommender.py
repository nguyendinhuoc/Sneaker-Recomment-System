import pandas as pd
from sqlalchemy import create_engine
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import os
from dotenv import load_dotenv

load_dotenv()

def train_item_based_model():
    print("--- 🧠 KHỞI ĐỘNG VÒNG LẶP AI ---")
    
    # 1. Kết nối "Ống dẫn" Database
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    
    # 2. Lấy nguyên liệu (Lịch sử tương tác)
    print("📥 1. Đang lấy dữ liệu hành vi người dùng...")
    query = "SELECT user_id, product_id, interaction_type FROM interactions"
    df = pd.read_sql(query, engine)
    
    if df.empty:
        print("⚠️ Chưa có tương tác nào. Hãy lên Web click xem và mua vài đôi giày trước nhé!")
        return

    # 3. Làm sạch và Chấm điểm (Scoring)
    print("⚖️ 2. Đang chấm điểm: View (1), Cart (3), Buy (5)...")
    scores = {'view': 1, 'add_to_cart': 3, 'buy': 5}
    df['score'] = df['interaction_type'].map(scores)
    
    # Nếu 1 người click xem 1 đôi giày 5 lần, ta cộng dồn điểm lại cho chính xác mức độ quan tâm
    df_grouped = df.groupby(['user_id', 'product_id'])['score'].sum().reset_index()

    # 4. Xây dựng Ma trận User-Item
    print("🧮 3. Đang xây dựng Ma trận Vector...")
    user_item_matrix = df_grouped.pivot(index='user_id', columns='product_id', values='score').fillna(0)

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
    else:
        print("⚠️ Dữ liệu chưa đủ để AI tìm ra quy luật. Hãy tạo thêm các kịch bản mua hàng chéo nhau!")

if __name__ == "__main__":
    train_item_based_model()