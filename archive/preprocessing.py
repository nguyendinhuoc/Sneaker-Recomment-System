# Data cleaning
import pandas as pd
import os


# CLEAN USERS
def clean_users():

    users = pd.read_csv("data/raw/users.csv")

    # remove duplicate users
    users = users.drop_duplicates(subset="user_id")

    # convert datetime
    users["created_at"] = pd.to_datetime(
        users["created_at"],
        errors="coerce"
    )

    # drop invalid rows
    users = users.dropna(subset=["user_id"])

    return users


# CLEAN INTERACTIONS
def clean_interactions():

    interactions = pd.read_csv("data/raw/user_interactions.csv")

    # convert datetime
    interactions["interaction_time"] = pd.to_datetime(
        interactions["interaction_time"],
        errors="coerce"
    )

    # normalize interaction type
    interactions["interaction_type"] = (
        interactions["interaction_type"]
        .astype(str)
        .str.lower()
        .str.strip()
    )

    # map interaction weight
    weight_map = {
        "view": 1,
        "like": 2,
        "addtocart": 3,
        "purchase": 5
    }

    interactions["weight"] = interactions["interaction_type"].map(weight_map)

    # remove unknown interaction types
    interactions = interactions.dropna(subset=["weight"])

    # remove duplicates
    interactions = interactions.drop_duplicates(
        subset=["user_id", "product_id", "interaction_time"]
    )

    # sort interactions
    interactions = interactions.sort_values(
        "interaction_time"
    )

    return interactions


# CLEAN PRODUCTS
def clean_products():

    products = pd.read_parquet("data/raw/products.parquet")

    # create product_id
    products = products.reset_index(drop=True)
    products.insert(0, "product_id", products.index + 1)

    # convert price
    products["price"] = pd.to_numeric(
        products["price"],
        errors="coerce"
    )

    # remove missing price
    products = products.dropna(subset=["price"])

    # remove unrealistic prices
    products = products[
        products["price"].between(10, 500)
    ]

    # round price
    products["price"] = products["price"].round(2)

    # convert USD -> VND
    exchange_rate = 24000
    products["price_vnd"] = (
        products["price"] * exchange_rate
    ).round(-3)

    # lowercase text columns
    text_cols = products.select_dtypes(include="object").columns

    for col in text_cols:
        products[col] = (
            products[col]
            .astype(str)
            .str.lower()
            .str.strip()
        )

    # remove remaining null rows
    products = products.dropna()

    return products


# MAIN 
if __name__ == "__main__":

    print("Starting preprocessing pipeline...")
    os.makedirs("data/processed", exist_ok=True)

    users = clean_users()
    interactions = clean_interactions()
    products = clean_products()

    # save processed datasets
    users.to_csv(
        "data/processed/users_clean.csv",
        index=False
    )

    interactions.to_csv(
        "data/processed/interactions_clean.csv",
        index=False
    )

    products.to_parquet(
        "data/processed/products_clean.parquet",
        index=False
    )

    print("Preprocessing completed!")
    print("Users:", len(users))
    print("Interactions:", len(interactions))
    print("Products:", len(products))