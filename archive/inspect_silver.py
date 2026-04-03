import pandas as pd
import os
from dotenv import load_dotenv

# Cấu hình Pandas để hiển thị đẹp trên Terminal (không bị ẩn cột)
pd.set_option('display.max_rows', 50)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

def main():
    load_dotenv()

    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    bucket_name = os.getenv("S3_BUCKET_NAME", "sneaker-db")

    # Đường dẫn file Parquet mà job cleaning vừa ghi ra
    parquet_path = f"s3://{bucket_name}/silver/sneakers_clean.parquet"

    print(f"📖 Đang kết nối S3 và đọc file: {parquet_path}")

    try:
        # Đọc trực tiếp từ S3
        df = pd.read_parquet(
            parquet_path,
            storage_options={"key": aws_key, "secret": aws_secret}
        )
        
        print(f"\n✅ Đọc thành công! Tổng số sản phẩm siêu sạch: {len(df)}")
        
        print("\n" + "="*80)
        print("🔍 10 SẢN PHẨM MẪU (Nhìn lướt qua các cột):")
        print("="*80)
        print(df.head(10).to_string())
        
        print("\n" + "="*80)
        print("📋 THỐNG KÊ NHANH (Xem có cột nào bị rỗng không):")
        print("="*80)
        df.info()

        # MẸO NHỎ: Lưu 1 bản CSV về máy tính local để bạn mở bằng Excel xem cho sướng mắt
        local_csv = "silver_data_sample.csv"
        df.to_csv(local_csv, index=False, encoding='utf-8')
        print(f"\n🎁 Đã lưu bản copy ra file '{local_csv}' ngay trong thư mục dự án.")
        print("👉 Bạn có thể click mở file này ngay trong VS Code hoặc Excel để xem toàn bộ dữ liệu!")

    except Exception as e:
        print(f"❌ Lỗi khi đọc file: {e}")

if __name__ == "__main__":
    main()