from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("store_id", "category").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("price", "id")
w_ordered = Window.partitionBy("product_id").orderBy("last_known_price", "id")
w5 = Window.partitionBy("product_id", "store_id").orderBy(F.col("price_bucket").desc())
w_recent = Window.partitionBy("store_id").orderBy("price", "id")

with_dims = (
    df_store_sales
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.price"), F.col("d.category"))
)

enriched = (
    with_dims
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.price").alias("product_price"))
)

with_windows = (
    enriched
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "price",
        "category",
        F.first("category", ignorenulls=True).over(w_grp).alias("first_known_category"),
        F.last("price", ignorenulls=True).over(w_hist).alias("last_known_price"),
    )
)

metrics = (
    with_windows
    .filter(F.col("sale_date") <= "2024-12-26")
    .select(
        "id",
        "store_id",
        "product_id",
        "price",
        "first_known_category",
        F.ntile(4).over(w3).alias("price_bucket"),
        F.row_number().over(w_ordered).alias("event_seq"),
    )
)

deduped = (
    metrics
    .withColumn("dedup_rank", F.rank().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "store_id", "price", "first_known_category", "price_bucket")
)

with_windows_store_sales = (
    deduped
    .withColumn("prev_price", F.lag("price", 1).over(w_recent))
    .withColumn("event_seq", F.row_number().over(w_recent))
)

result = (
    with_windows_store_sales
    .cube(
        "store_id",
        F.when(F.col("price_bucket") >= 7, "HIGH").otherwise("LOW").alias("price_bucket_tier"),
    )
    .agg(
        F.percentile_approx("price", 0.9).alias("price_p90"),
        F.percentile_approx("price_bucket", [0.25, 0.5, 0.75]).alias("price_bucket_quartiles"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.percentile_approx("price", 0.95).alias("price_p95"),
        F.percentile_approx("event_seq", 0.9).alias("event_seq_p90"),
    )
    .filter(F.col("price_p90") >= 0)
    .orderBy(F.col("price_bucket_tier").desc_nulls_first())
)
