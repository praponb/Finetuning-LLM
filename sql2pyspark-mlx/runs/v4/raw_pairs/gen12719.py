from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id")
w2 = Window.partitionBy("brand", "category").orderBy("launch_date", "id")
w3 = Window.partitionBy("brand").orderBy(F.col("launch_date").desc(), "id")
w4 = Window.partitionBy("brand").orderBy("cost", "id")
w5 = Window.partitionBy("category", "brand").orderBy(F.col("next_cost").desc_nulls_first(), "id")

cleaned = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.col("cost").between(260, 470))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        F.substring("category", 1, 4).alias("category_prefix"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.lag("price", 2).over(w).alias("prev_price"),
        F.ntile(4).over(w).alias("price_bucket"),
        F.lead("cost", 2, 0).over(w2).alias("next_cost"),
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("seq_no_2"),
    )
)

latest = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

filtered = (
    latest
    .filter(F.col("price") >= 860)
    .select(
        "*",
        F.array_contains("tags", "sale").alias("is_sale"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_clean"),
    )
)

result = (
    filtered
    .select(
        "id",
        "brand",
        "cost",
        "category_prefix",
        "price_bucket",
        "next_cost",
        "latest_rank",
        "is_sale",
        "brand_clean",
    )
    .orderBy(F.col("next_cost").desc_nulls_last(), F.col("brand").desc())
)
