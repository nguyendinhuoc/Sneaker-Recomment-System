import os
import pandas as pd
import numpy as np

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import MinMaxScaler

# LOAD DATA
def load_data():
    load_dotenv()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL không tồn tại trong file .env")

    engine = create_engine(db_url)

    products_query = """
        SELECT
            product_id,
            name,
            brand,
            category,
            style,
            type,
            purpose,
            color,
            material,
            price,
            image_url,
            source_url
        FROM products
    """

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

    products = pd.read_sql(products_query, engine)
    interactions = pd.read_sql(interactions_query, engine)

    products["product_id"] = products["product_id"].astype(str)
    interactions["product_id"] = interactions["product_id"].astype(str)

    return products, interactions


# SELECT FEATURES
FEATURE_COLUMNS = [
    "brand",
    "category",
    "style",
    "type",
    "purpose",
    "color",
    "material"
]


# ENCODE CATEGORICAL FEATURES
def encode_categorical(products):

    # đảm bảo các cột tồn tại
    missing_cols = [c for c in FEATURE_COLUMNS if c not in products.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in products: {missing_cols}")

    encoder = OneHotEncoder(
        sparse_output=False,
        handle_unknown="ignore"
    )

    encoded_array = encoder.fit_transform(products[FEATURE_COLUMNS])

    encoded_df = pd.DataFrame(
        encoded_array,
        columns=encoder.get_feature_names_out(FEATURE_COLUMNS),
        index=products.index
    )

    return encoded_df


# SCALE PRICE
def scale_price(products):

    scaler = MinMaxScaler()

    price_scaled = scaler.fit_transform(products[["price"]])

    price_df = pd.DataFrame(
        price_scaled,
        columns=["price_scaled"],
        index=products.index
    )

    return price_df


# COMPUTE PRODUCT POPULARITY
def compute_popularity(interactions):
    view_count = (
        interactions[interactions["interaction_type"] == "view"]
        .groupby("product_id")
        .size()
        .reset_index(name="view_count")
    )

    like_count = (
        interactions[interactions["interaction_type"] == "like"]
        .groupby("product_id")
        .size()
        .reset_index(name="like_count")
    )

    add_to_cart_count = (
        interactions[interactions["interaction_type"] == "add_to_cart"]
        .groupby("product_id")
        .size()
        .reset_index(name="add_to_cart_count")
    )

    purchase_count = (
        interactions[interactions["interaction_type"] == "purchase"]
        .groupby("product_id")
        .size()
        .reset_index(name="purchase_count")
    )

    popularity = view_count.merge(like_count, on="product_id", how="outer")
    popularity = popularity.merge(add_to_cart_count, on="product_id", how="outer")
    popularity = popularity.merge(purchase_count, on="product_id", how="outer")

    popularity = popularity.fillna(0)

    for col in ["view_count", "like_count", "add_to_cart_count", "purchase_count"]:
        popularity[col] = popularity[col].astype(int)

    return popularity


# BUILD PRODUCT FEATURE VECTOR
def build_product_features(products, interactions):

    print("Encoding categorical features...")
    categorical_features = encode_categorical(products)

    print("Scaling price feature...")
    price_feature = scale_price(products)

    print("Computing product popularity...")
    popularity = compute_popularity(interactions)

    print("Combining features...")

    product_features = pd.concat(
        [
            products[["product_id"]],
            categorical_features,
            price_feature
        ],
        axis=1
    )

    product_features = product_features.merge(
        popularity,
        on="product_id",
        how="left"
    )

    product_features = product_features.fillna(0)

    return product_features


# SAVE FEATURE STORE
def save_features(product_features):

    path = "data/feature_store/product_features.parquet"

    product_features.to_parquet(path, index=False)

    print("Saved feature store to:", path)


# MAIN 
def main():

    print("Loading data...")

    products, interactions = load_data()

    print("Products:", products.shape)
    print("Interactions:", interactions.shape)

    product_features = build_product_features(products, interactions)

    print("Feature shape:", product_features.shape)

    save_features(product_features)

    print("\nSample features:")
    print(product_features.head())


if __name__ == "__main__":
    main()