from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("category")
w6 = Window.partitionBy("category").orderBy(F.col("quantity_bucket").desc_nulls_last())
w7 = Window.partitionBy("category").orderBy(F.col("category_sum_unit_price_share").desc_nulls_last())

with_windows = (
    df_order_items
    .select(
        "id",
        "product_id",
        F.ntile(5).over(w).alias("quantity_bucket"),
        F.round((F.col("quantity") / F.sum("quantity").over(w2)), 4).alias("quantity_share"),
        F.round((F.col("quantity") / F.sum("quantity").over(w3)), 4).alias("quantity_share_2"),
        F.first("unit_price", ignorenulls=True).over(w4).alias("first_known_unit_price"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4).alias("unit_price_share"),
    )
)

joined = (
    with_windows
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.category"), F.col("p.price"))
)

matched_keys = (
    joined
    .select("id")
    .subtract(
        df_order_items
        .filter(F.col("product_id") == 12)
        .select("id")
    )
)

verified = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "quantity_bucket",
        "category",
        "price",
        F.count("first_known_unit_price").over(w_grp).alias("category_count_first_known_unit_price"),
        F.sum("unit_price_share").over(w_grp).alias("category_sum_unit_price_share"),
    )
)

result = (
    scored
    .filter(F.col("category") != "Home")
    .select(
        "category",
        "price",
        "category_count_first_known_unit_price",
        F.min("category_sum_unit_price_share").over(w_grp).alias("category_min_category_sum_unit_price_share"),
        F.round((F.col("quantity_bucket") / F.sum("quantity_bucket").over(w_grp)), 4).alias("quantity_bucket_share"),
        F.rank().over(w6).alias("quantity_bucket_rank"),
        F.dense_rank().over(w7).alias("category_sum_unit_price_share_dense_rank"),
    )
)
