import subprocess
import time
import sys
import os
from datetime import datetime

# --- CẤU HÌNH ---
# Đường dẫn tương đối từ thư mục gốc (sneaker-recsys)
CRAWLER_PATH = os.path.join("src", "ingestion", "crawler.py")
CLEANER_PATH = os.path.join("src", "transformation", "silver_cleaning.py")

# Thời gian nghỉ giữa các lần chạy (Giây). Ví dụ: 6 tiếng = 21600s
SLEEP_TIME = 21600 

def run_step(script_path, step_name):
    """Hàm chạy một script con và kiểm tra lỗi"""
    print(f"\n{'-'*60}")
    print(f"🚀 [BẮT ĐẦU] {step_name}")
    print(f"📜 Script: {script_path}")
    print(f"⏰ Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    if not os.path.exists(script_path):
        print(f"❌ LỖI: Không tìm thấy file tại {script_path}")
        return False

    try:
        # Gọi script bằng python hiện tại (sys.executable)
        result = subprocess.run(
            [sys.executable, script_path],
            check=True, # Sẽ báo lỗi nếu script con trả về exit code != 0
            text=True
        )
        print(f"✅ [HOÀN THÀNH] {step_name}")
        return True
        
        
    except subprocess.CalledProcessError as e:
        print(f"❌ [THẤT BẠI] {step_name} gặp lỗi! (Exit Code: {e.returncode})")
        return False
    except Exception as e:
        print(f"❌ [LỖI HỆ THỐNG] {str(e)}")
        return False

def pipeline_job():
    """Quy trình chạy 1 lần Full Pipeline"""
    start = time.time()
    print(f"\n{'='*60}")
    print(f"🎬 BẮT ĐẦU PIPELINE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    # BƯỚC 1: CÀO DỮ LIỆU (Bronze)
    # Nhiệm vụ: Lấy dữ liệu mới nhất từ eBay về S3 Bronze
    if not run_step(CRAWLER_PATH, "Bước 1: Cào Dữ Liệu (Crawler)"):
        print("⛔ Pipeline dừng lại do lỗi Cào dữ liệu.")
        return

    # BƯỚC 2: LÀM SẠCH (Silver)
    # Nhiệm vụ: Đọc Bronze, làm sạch, ghi đè Silver
    if not run_step(CLEANER_PATH, "Bước 2: Làm Sạch Dữ Liệu (Spark Cleaning)"):
        print("⛔ Pipeline dừng lại do lỗi Làm sạch.")
        return

    duration = time.time() - start
    print(f"\n🎉 PIPELINE HOÀN TẤT! Tổng thời gian: {duration:.2f} giây")
    print(f"✨ Dữ liệu tại S3 Silver đã sẵn sàng cho Machine Learning.")

if __name__ == "__main__":
    print("🤖 HỆ THỐNG TỰ ĐỘNG CÀO & XỬ LÝ DỮ LIỆU ĐÃ KÍCH HOẠT")
    print(f"💤 Chu kỳ chạy: {SLEEP_TIME/3600} tiếng/lần")
    
    try:
        while True:
            pipeline_job()
            
            # Đếm ngược thời gian nghỉ
            print(f"\n😴 Đang ngủ... Lần chạy tiếp theo sau {SLEEP_TIME/3600} tiếng.")
            time.sleep(SLEEP_TIME)
            
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng hệ thống thủ công.")