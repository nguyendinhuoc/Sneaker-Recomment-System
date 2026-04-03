import os
import pandas as pd
import numpy as np

from dotenv import load_dotenv
from sqlalchemy import create_engine

# 1. Định nghĩa bảng trọng số (Action Weights) - Cần khớp với Database
ACTION_WEIGHTS = {
    'view': 1,
    'like': 2,
    'add_to_cart': 3,
    'purchase': 5
}

# LOAD DATA TỪ LOCAL PROCESSED / FEATURE STORE
def load_data():
    print("--- 1. Đang đọc dữ liệu tương tác từ Neon SQL ---")
    load_dotenv()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL không tồn tại trong file .env")

    engine = create_engine(db_url)

    interactions_query = """
        SELECT
            interaction_id,
            user_id,
            product_id,
            interaction_type,
            quantity,
            interaction_time
        FROM interactions
    """

    interactions = pd.read_sql(interactions_query, engine)

    required_interaction_cols = ["user_id", "product_id", "interaction_type"]
    missing_interaction_cols = [c for c in required_interaction_cols if c not in interactions.columns]
    if missing_interaction_cols:
        raise ValueError(
            f"Thiếu cột trong dữ liệu interactions từ Neon: {missing_interaction_cols}"
        )

    interactions = interactions.rename(columns={"interaction_type": "action"})
    interactions["user_id"] = interactions["user_id"].astype(str)
    interactions["product_id"] = interactions["product_id"].astype(str)

    if "interaction_time" not in interactions.columns:
        interactions["interaction_time"] = pd.NaT

    print("--- 2. Đang đọc đặc trưng sản phẩm từ Parquet ---")
    product_features_path = "data/feature_store/product_features.parquet"
    if not os.path.exists(product_features_path):
        raise FileNotFoundError(
            f"Không tìm thấy file {product_features_path}. "
            "Hãy chạy feature_engineering.py trước."
        )

    product_features = pd.read_parquet(product_features_path)
    product_features["product_id"] = product_features["product_id"].astype(str)

    return interactions, product_features


def join_interactions_products(interactions, product_features):
    print("--- 3. Đang ánh xạ trọng số (weights) cho từng hành động ---")
    # Ánh xạ chữ (view/purchase) sang số (1/5)
    interactions['weight'] = interactions['action'].map(ACTION_WEIGHTS).fillna(0)
    
    # Thực hiện merge
    df = interactions.merge(
        product_features,
        on="product_id",
        how="left"
    )
    
    return df.copy()


def compute_user_vectors(df):
    # Loại bỏ các cột không phải đặc trưng tính toán (ID, Time, Cột chữ gốc)
    exclude_cols = [
        "user_id", "product_id", "interaction_type", "action",
        "interaction_time", "interaction_id", "quantity", "weight",
        "brand", "category", "style", "type", "purpose", "color", "material"
    ]

    # Chỉ chọn cột SỐ để tránh lỗi TypeError: 'str' / 'int'
    feature_df = df.select_dtypes(include=[np.number]).drop(columns=[c for c in exclude_cols if c in df.columns], errors='ignore')
    
    print(f"--- 4. Đang tính toán trên {len(feature_df.columns)} đặc trưng số ---")

    # Nhân đặc trưng với trọng số hành động
    weighted_features = feature_df.mul(df["weight"], axis=0)
    
    # Gom nhóm theo user và tính tổng vector
    user_vectors = weighted_features.groupby(df["user_id"]).sum()
    
    # Chia cho tổng trọng số để ra Vector trung bình (User Profile)
    weight_sum = df.groupby("user_id")["weight"].sum()
    weight_sum = weight_sum.replace(0, 1) # Tránh lỗi chia cho 0
    
    user_vectors = user_vectors.div(weight_sum, axis=0)
    return user_vectors.reset_index()


def compute_price_preference(df):
    # Tính mức giá trung bình mà user thường xem/mua
    price_pref = (
        df.groupby("user_id")["price_scaled"]
        .mean()
        .reset_index()
    )
    price_pref = price_pref.rename(columns={"price_scaled": "mean_price_pref"})
    return price_pref


def build_user_profiles(interactions, product_features):
    print("--- Bắt đầu xây dựng User Profiles ---")
    df = join_interactions_products(interactions, product_features)

    print("Computing user vectors...")
    user_vectors = compute_user_vectors(df)

    print("Computing price preference...")
    price_pref = compute_price_preference(df)

    print("Combining all features...")
    user_profiles = user_vectors.merge(
        price_pref,
        on="user_id",
        how="left"
    )

    return user_profiles


def save_user_profiles(user_profiles):
    os.makedirs("data/feature_store", exist_ok=True)
    path = "data/feature_store/user_profiles.parquet"
    user_profiles.to_parquet(path, index=False)
    print(f"✅ Đã lưu file backup: {path}")


def save_user_profiles_to_neon(user_profiles):
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    # pool_pre_ping giúp kiểm tra kết nối còn sống hay không trước khi gửi dữ liệu
    engine = create_engine(db_url, pool_pre_ping=True)
    
    print("--- 5. Đang đẩy User Profiles lên Neon SQL (Schema: public) ---")
    
    try:
        with engine.begin() as connection:
            # Ép kiểu user_id về string để tránh lệch kiểu dữ liệu khi JOIN
            user_profiles['user_id'] = user_profiles['user_id'].astype(str)
            
            user_profiles.to_sql(
                'user_profiles', 
                con=connection, 
                if_exists='replace', 
                index=False,
                schema='public' # ÉP VÀO SCHEMA PUBLIC
            )
        print("✅ Dữ liệu đã được COMMIT thành công!")

        # Kiểm tra xác thực ngay lập tức
        with engine.connect() as conn:
            result = conn.execute(text("SELECT count(*) FROM public.user_profiles"))
            print(f"📊 Xác nhận thực tế: Đã có {result.fetchone()[0]} dòng trong bảng 'user_profiles'.")
            
    except Exception as e:
        print(f"❌ Lỗi khi đẩy dữ liệu lên Neon: {e}")
        raise e


def main():
    print("\n" + "="*50)
    print("BẮT ĐẦU QUY TRÌNH XỬ LÝ USER PROFILE")
    print("="*50)
    
    try:
        # 1. Load dữ liệu
        interactions, product_features = load_data()
        print(f"Dữ liệu đầu vào: {len(interactions)} interactions, {len(product_features)} products.")

        # 2. Xây dựng User Profiles (Phải tính xong mới có biến để lưu)
        user_profiles = build_user_profiles(interactions, product_features)
        print(f"Xử lý hoàn tất! Kích thước Profile: {user_profiles.shape}")

        # 3. Lưu file backup cục bộ
        save_user_profiles(user_profiles)
        
        # 4. Đẩy lên Neon SQL
        # save_user_profiles_to_neon(user_profiles)

        print("="*50)
        print("HOÀN THÀNH: ĐÃ TẠO FILE data/feature_store/user_profiles.parquet")
        print("="*50 + "\n")

    except Exception as e:
        print(f"❌ LỖI TRONG QUÁ TRÌNH XỬ LÝ: {e}")

if __name__ == "__main__":
    main()