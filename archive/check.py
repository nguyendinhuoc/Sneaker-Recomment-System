import pandas as pd
import s3fs
import os
from dotenv import load_dotenv

# 1. Nạp biến môi trường từ file .env
load_dotenv()

# 2. Khởi tạo kết nối S3 với thông tin đăng nhập cụ thể
fs = s3fs.S3FileSystem(
    key=os.getenv("AWS_ACCESS_KEY_ID"),
    secret=os.getenv("AWS_SECRET_ACCESS_KEY")
)

def check_gold_schema(folder_name):
    # Lưu ý: Bỏ s3:// nếu dùng trực tiếp với fs.glob hoặc giữ nguyên tùy phiên bản
    path = f"sneaker-db/gold/{folder_name}/"
    print(f"\n--- 🔍 Kiểm tra thư mục Gold: {folder_name} ---")
    
    try:
        # Tìm danh sách file .parquet
        files = fs.glob(path + "*.parquet")
        if not files:
            print(f"⚠️ Không tìm thấy file nào trong {path}")
            return
        
        # Đọc thử file đầu tiên tìm thấy
        with fs.open(files[0]) as f:
            df = pd.read_parquet("s3://sneaker-db/silver/products/products_clean.parquet")
            print(df.columns.tolist())
            print(df.head(1))
            print(f"✅ Danh sách cột: {df.columns.tolist()}")
            print("--- Dữ liệu mẫu (3 dòng đầu) ---")
            print(df.head(3))
            
    except Exception as e:
        print(f"❌ Lỗi thực thi: {e}")

if __name__ == "__main__":
    # Kiểm tra 2 folder quan trọng nhất cho trí tưởng tượng của bạn
    check_gold_schema("item_similarity")
    check_gold_schema("recommendations")