from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("store_sqft", "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("store_sqft").desc())

joined = (
    df_store_sales
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.sale_date",
        F.col("b.sqft"),
        F.col("b.store_name"),
    )
)

with_dims = (
    joined
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .select(
        "src.*",
        F.col("b.opened_date"),
        F.col("b.store_name").alias("store_store_name"),
        F.col("b.sqft").alias("store_sqft"),
    )
)

with_windows = (
    with_dims
    .select(
        "id",
        "sale_date",
        "sqft",
        "opened_date",
        "store_store_name",
        F.ntile(3).over(w).alias("store_sqft_bucket"),
        F.rank().over(w2).alias("store_sqft_rank"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("sale_date") >= F.lit("2023-05-05").cast("date"))
    .select(
        "id",
        "sale_date",
        "opened_date",
        "store_sqft_bucket",
        "store_sqft_rank",
        (F.col("sqft") - F.col("store_sqft_bucket")).alias("sqft_minus_store_sqft_bucket"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("store_sqft_bucket") >= 8, "PREMIUM").otherwise("STANDARD").alias("store_sqft_bucket_tier"),
    )
    .agg(
        F.round(F.avg("sqft_minus_store_sqft_bucket"), 2).alias("avg_sqft_minus_store_sqft_bucket"),
        F.percentile_approx("store_sqft_bucket", 0.95).alias("store_sqft_bucket_p95"),
        F.percentile_approx("store_sqft_bucket", 0.9).alias("store_sqft_bucket_p90"),
        F.avg("store_sqft_rank").alias("avg_store_sqft_rank"),
        F.round(F.avg("sqft_minus_store_sqft_bucket"), 2).alias("avg_sqft_minus_store_sqft_bucket_2"),
        F.percentile_approx("sqft_minus_store_sqft_bucket", [0.25, 0.5, 0.75]).alias("sqft_minus_store_sqft_bucket_quartiles"),
    )
    .filter(F.col("store_sqft_bucket_p95") < 1000000)
)
