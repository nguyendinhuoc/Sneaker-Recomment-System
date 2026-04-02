import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import psycopg2
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

def load_features():
    # Đọc kết quả từ Bước 2
    user_profiles = pd.read_parquet("data/feature_store/user_profiles.parquet")
    product_features = pd.read_parquet("data/feature_store/product_features.parquet")
    
    # Đảm bảo ID là string để không bị lệch kiểu
    user_profiles['user_id'] = user_profiles['user_id'].astype(str)
    product_features['product_id'] = product_features['product_id'].astype(str)
    
    return user_profiles, product_features

def generate_recommendations(user_profiles, product_features, top_k=10):
    print(f"--- Đang tính toán gợi ý cho {len(user_profiles)} người dùng ---")
    
    # Tách ID và Vector để tính toán
    user_ids = user_profiles['user_id'].values
    user_vectors = user_profiles.drop(columns=['user_id', 'mean_price_pref'], errors='ignore').values
    
    product_ids = product_features['product_id'].values
    product_vectors = product_features.drop(columns=['product_id'], errors='ignore').values
    
    # Tính toán độ tương đồng Cosine giữa Người dùng và Sản phẩm
    # Kết quả là một ma trận (Số User x Số Product)
    similarity_matrix = cosine_similarity(user_vectors, product_vectors)
    
    recs = []
    for i, user_id in enumerate(user_ids):
        # Lấy top_k sản phẩm có điểm cao nhất cho từng user
        sim_scores = similarity_matrix[i]
        top_indices = sim_scores.argsort()[-top_k:][::-1]
        
        for idx in top_indices:
            recs.append({
                'user_id': user_id,
                'product_id': product_ids[idx],
                'score': float(sim_scores[idx])
            })
            
    return pd.DataFrame(recs)

def save_to_neon(recommendations):
    load_dotenv()
    engine = create_engine(os.getenv("DATABASE_URL"))
    
    print("--- Đang đẩy kết quả gợi ý lên tầng GOLD (Neon SQL) ---")
    # Lưu vào bảng gold_user_recommendations
    recommendations.to_sql(
        'gold_user_recommendations', 
        engine, 
        if_exists='replace', 
        index=False
    )
    print("✅ Đã cập nhật bảng Gold trên Neon!")

def main():
    user_profiles, product_features = load_features()
    recommendations = generate_recommendations(user_profiles, product_features)
    save_to_neon(recommendations)

if __name__ == "__main__":
    main()