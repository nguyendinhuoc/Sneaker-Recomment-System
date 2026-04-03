import json
import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os

# --- CẤU HÌNH ---
INPUT_PRODUCT_FILE = "data/raw/amazon_shoes_final.json"
OUTPUT_DIR = "data/synthetic"
NUM_USERS = 1000          # Giả lập 1000 người dùng
NUM_INTERACTIONS = 5000   # Giả lập 5000 lượt tương tác

fake = Faker()
# Seed để mỗi lần chạy dữ liệu ra giống nhau (dễ debug)
Faker.seed(42)
random.seed(42)
np.random.seed(42)

def generate_users(num_users):
    """
    Sinh ra bảng users dựa trên schema: user_id, name, gender, age, created_at
    """
    print(f"👥 Đang sinh {num_users} người dùng giả...")
    users = []
    genders = ['Male', 'Female', 'Other']

    for i in range(1, num_users + 1):
        gender = np.random.choice(genders, p=[0.45, 0.45, 0.1])
        
        # Logic: Tạo tên theo giới tính cho thật
        if gender == 'Male':
            name = fake.name_male()
        elif gender == 'Female':
            name = fake.name_female()
        else:
            name = fake.name()
            
        users.append({
            "user_id": i,
            "name": name,
            "gender": gender,
            "age": random.randint(16, 60), # Tuổi mua giày từ 16-60
            "created_at": fake.date_time_between(start_date='-2y', end_date='now')
        })
    
    return pd.DataFrame(users)

def generate_interactions(users_df, products_data, num_interactions):
    """
    Sinh ra bảng interactions: interaction_id, user_id, product_id, interaction_type...
    """
    print(f"🖱️ Đang sinh {num_interactions} lượt tương tác giả...")
    
    interactions = []
    
    # Lấy list user_id
    user_ids = users_df['user_id'].tolist()
    
    # Tạo mapping product_id giả cho dữ liệu JSON (đánh số từ 1 đến N)
    # Vì file JSON chưa có ID số nguyên, ta tạm gán index làm ID
    product_ids = list(range(1, len(products_data) + 1))
    
    # Các loại tương tác và trọng số (View nhiều hơn Buy)
    # View: Xem, Cart: Thêm giỏ, Purchase: Mua, Like: Thích
    action_types = ['View', 'AddToCart', 'Purchase', 'Like']
    action_weights = [0.7, 0.15, 0.1, 0.05] # 70% là xem, chỉ 10% là mua
    
    for i in range(1, num_interactions + 1):
        # 1. Chọn random User
        u_id = random.choice(user_ids)
        
        # 2. Chọn random Product (Giày)
        p_id = random.choice(product_ids)
        
        # 3. Chọn hành động
        act = np.random.choice(action_types, p=action_weights)
        
        # 4. Sinh thời gian (phải sau ngày tạo tài khoản của user đó)
        # Để đơn giản, ta cứ lấy random trong 3 tháng gần nhất
        inter_time = fake.date_time_between(start_date='-3M', end_date='now')
        
        interactions.append({
            "interaction_id": i,
            "user_id": u_id,
            "product_id": p_id,
            "interaction_type": act,
            "quantity": 1 if act != 'Purchase' else random.randint(1, 2),
            "interaction_time": inter_time
        })
        
    return pd.DataFrame(interactions)

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 1. Đọc dữ liệu Product đã cào
    if not os.path.exists(INPUT_PRODUCT_FILE):
        print("❌ Lỗi: Không tìm thấy file JSON giày. Chạy crawler trước đi!")
        return

    with open(INPUT_PRODUCT_FILE, 'r', encoding='utf-8') as f:
        products_data = json.load(f)
    print(f"✅ Đã tải {len(products_data)} sản phẩm từ Crawler.")
    
    # 2. Sinh Users
    df_users = generate_users(NUM_USERS)
    df_users.to_csv(f"{OUTPUT_DIR}/users.csv", index=False)
    print(f"   -> Lưu file: {OUTPUT_DIR}/users.csv")
    
    # 3. Sinh Interactions
    df_interactions = generate_interactions(df_users, products_data, NUM_INTERACTIONS)
    df_interactions.to_csv(f"{OUTPUT_DIR}/user_interactions.csv", index=False)
    print(f"   -> Lưu file: {OUTPUT_DIR}/user_interactions.csv")
    
    print("\n🎉 HOÀN THÀNH SINH DỮ LIỆU GIẢ!")

if __name__ == "__main__":
    main()