from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("supplier_id")
w3 = Window.partitionBy("brand").orderBy("cost", "id")
w4 = Window.partitionBy("brand").orderBy(F.col("category_length").desc(), "id")
w5 = Window.partitionBy("brand").orderBy("rank_in_group", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("brand").orderBy(F.col("prev_cost").desc(), "id")

valid = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-06-07"))) & F.array_contains("tags", "eco"))
    .select("*", F.array_contains("tags", "eco").alias("is_eco"))
)

enriched = (
    valid
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "tags",
        F.corr("cost", "stock_qty").over(w).alias("corr_cost_stock_qty"),
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
        F.lag("cost", 1).over(w3).alias("prev_cost"),
    )
)

cleaned = (
    enriched
    .filter(F.col("brand").eqNullSafe(F.lit("Stark")))
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "tags",
        "price_share",
        "prev_cost",
        F.length("category").alias("category_length"),
        F.substring("category", 1, 4).alias("category_prefix"),
    )
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "tags",
        "price_share",
        "category_prefix",
        F.count("category_length").over(w5).alias("last7_count_category_length"),
        F.min("category_length").over(w5).alias("last7_min_category_length"),
        F.max("category_length").over(w5).alias("last7_max_category_length"),
        F.row_number().over(w6).alias("row_seq"),
    )
)
