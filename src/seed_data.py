import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def seed_interactions():
    db_url = os.getenv("DATABASE_URL")
    aws_creds = {
        "key": os.getenv("AWS_ACCESS_KEY_ID"),
        "secret": os.getenv("AWS_SECRET_ACCESS_KEY")
    }
    engine = create_engine(db_url)

    path_interaction = 's3://sneaker-db/silver/interactions/interactions_clean.parquet' 
    path_user = 's3://sneaker-db/silver/users/users_clean.parquet' 
    
    try:
        with engine.connect() as conn:
            print("🧹 Đang dọn dẹp dữ liệu cũ trên Neon (CASCADE)...")
            # Lệnh này xóa dữ liệu trong bảng nhưng giữ nguyên cấu trúc bảng
            # TRUNCATE CASCADE sẽ tự động dọn dẹp các bảng liên quan
            conn.execute(text("TRUNCATE TABLE users, interactions, orders RESTART IDENTITY CASCADE;"))
            conn.commit()

        # --- NẠP USERS ---
        print(f"📖 Đang đọc Users từ S3...")
        df_users = pd.read_parquet(path_user, storage_options=aws_creds)
        print("🚀 Đang nạp Users lên Neon...")
        # Dùng 'append' vì bảng đã được TRUNCATE sạch sẽ ở trên rồi
        df_users.to_sql('users', engine, if_exists='append', index=False)
        print(f"✅ Đã nạp {len(df_users)} users.")

        # --- NẠP INTERACTIONS ---
        print(f"📖 Đang đọc Interactions từ S3...")
        df_interactions = pd.read_parquet(path_interaction, storage_options=aws_creds)
        print("🚀 Đang nạp Interactions lên Neon...")
        df_interactions.to_sql('interactions', engine, if_exists='append', index=False)
        print(f"✅ Đã nạp {len(df_interactions)} tương tác.")

    except Exception as e:
        print(f"❌ Lỗi: {e}")

if __name__ == "__main__":
    seed_interactions()