import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

def load_features():
    user_profiles_path = "data/feature_store/user_profiles.parquet"
    product_features_path = "data/feature_store/product_features.parquet"

    if not os.path.exists(user_profiles_path):
        raise FileNotFoundError(
            f"Không tìm thấy {user_profiles_path}. Hãy chạy user_profile.py trước."
        )

    if not os.path.exists(product_features_path):
        raise FileNotFoundError(
            f"Không tìm thấy {product_features_path}. Hãy chạy feature_engineering.py trước."
        )

    user_profiles = pd.read_parquet(user_profiles_path)
    product_features = pd.read_parquet(product_features_path)

    user_profiles["user_id"] = user_profiles["user_id"].astype(str)
    product_features["product_id"] = product_features["product_id"].astype(str)

    return user_profiles, product_features

def generate_recommendations(user_profiles, product_features, top_k=10):
    print(f"--- Đang tính toán gợi ý cho {len(user_profiles)} người dùng ---")

    # Tách ID
    user_ids = user_profiles["user_id"].values
    product_ids = product_features["product_id"].values

    # Vector chính để tính cosine
    user_vectors = user_profiles.drop(columns=["user_id", "mean_price_pref"], errors="ignore").values
    product_vectors = product_features.drop(
        columns=["product_id", "view_count", "purchase_count"],
        errors="ignore"
    ).values

    # Tính cosine similarity giữa user và product
    similarity_matrix = cosine_similarity(user_vectors, product_vectors)

    # Giá ưa thích của user và giá scaled của product
    user_price_pref = user_profiles["mean_price_pref"].fillna(0).values
    product_price_scaled = product_features["price_scaled"].fillna(0).values

    # Chuẩn hóa popularity về [0, 1]
    max_view = max(product_features["view_count"].max(), 1)
    max_purchase = max(product_features["purchase_count"].max(), 1)

    normalized_view = product_features["view_count"].fillna(0).values / max_view
    normalized_purchase = product_features["purchase_count"].fillna(0).values / max_purchase

    popularity_score = 0.4 * normalized_view + 0.6 * normalized_purchase

    recs = []
    for i, user_id in enumerate(user_ids):
        cosine_scores = similarity_matrix[i]

        # Bonus giá
        price_bonus = 1 - np.abs(user_price_pref[i] - product_price_scaled)
        price_bonus = np.clip(price_bonus, 0, 1)

        # Final score
        final_scores = (
            0.75 * cosine_scores +
            0.15 * price_bonus +
            0.10 * popularity_score
        )

        top_indices = final_scores.argsort()[-top_k:][::-1]

        for idx in top_indices:
            recs.append({
                "user_id": user_id,
                "product_id": product_ids[idx],
                "score": float(final_scores[idx]),
                "cosine_score": float(cosine_scores[idx]),
                "price_bonus": float(price_bonus[idx]),
                "popularity_score": float(popularity_score[idx]),
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