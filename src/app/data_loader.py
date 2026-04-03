import pandas as pd


# LOAD DATA
def load_data():

    products = pd.read_parquet(
        "data/processed/products_clean.parquet"
    )

    features = pd.read_parquet(
        "data/feature_store/product_features.parquet"
    )

    recommendations = pd.read_parquet(
        "data/feature_store/recommendations.parquet"
    )

    item_similarity = pd.read_parquet(
        "data/feature_store/item_similarity.parquet"
    )

    # merge metadata + features
    if "product_id" in features.columns:

        products = products.merge(
            features,
            on="product_id",
            how="left"
        )

    # fallback popularity
    if "popularity" not in products.columns:

        products["popularity"] = range(len(products), 0, -1)

    return products, recommendations, item_similarity


# PRINT PRODUCTS
def print_products(df):

    cols = []

    for c in ["product_id", "brand", "category", "price"]:

        if c in df.columns:
            cols.append(c)

    print(df[cols])


# HOMEPAGE
def show_homepage(products):

    print("\n===== HOMEPAGE =====")

    top_products = products.sort_values(
        "popularity",
        ascending=False
    ).head(20)

    print("\nTop Popular Products\n")

    print_products(top_products)


# PRODUCT DETAIL
def show_product_detail(products, product_id):

    product = products[
        products["product_id"] == product_id
    ]

    if product.empty:

        print("Product not found")
        return

    p = product.iloc[0]

    print("\n===== PRODUCT DETAIL =====")

    fields = ["brand", "category", "price", "material", "purpose"]

    for f in fields:

        if f in products.columns:

            print(f.capitalize(), ":", p[f])


# RELATED PRODUCTS
def show_related_products(similarity, products, product_id):

    print("\n===== RELATED PRODUCTS =====")

    similar = similarity[
        similarity["product_id"] == product_id
    ]

    if similar.empty:

        print("No related products")
        return

    # detect similarity column
    sim_col = None

    for c in ["similarity", "score", "cosine_score", "sim_score"]:

        if c in similar.columns:
            sim_col = c
            break

    if sim_col is None:

        print("No similarity column found")
        print(similar.columns)
        return

    similar = similar.sort_values(
        sim_col,
        ascending=False
    ).head(5)

    ids = similar["similar_product_id"]

    related = products[
        products["product_id"].isin(ids)
    ]

    print_products(related)


# USER RECOMMENDATIONS
def show_user_recommendations(rec, products, user_id):

    print("\n===== RECOMMENDED FOR YOU =====")

    user_rec = rec[
        rec["user_id"] == user_id
    ]

    if user_rec.empty:

        print("No recommendations")
        return

    ids = user_rec["product_id"].head(10)

    rec_products = products[
        products["product_id"].isin(ids)
    ]

    print_products(rec_products)


# TRENDING
def show_trending(products):

    print("\n===== TRENDING PRODUCTS =====")

    trending = products.sort_values(
        "popularity",
        ascending=False
    ).head(10)

    print_products(trending)


# MAIN
def main():

    products, rec, sim = load_data()

    print("\nLOGIN")

    user_id = int(input("Enter user id: "))

    show_homepage(products)

    product_id = int(input("\nEnter product id to view: "))

    show_product_detail(products, product_id)

    show_related_products(sim, products, product_id)

    show_user_recommendations(rec, products, user_id)

    show_trending(products)


if __name__ == "__main__":

    main()