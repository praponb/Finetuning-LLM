from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy(F.col("product_id_count_quantity_filled").desc())
w4 = Window.partitionBy("product_id").orderBy("quantity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("quantity_bucket").asc_nulls_last())
w_ordered = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_id").orderBy(F.col("quantity_filled").desc(), "id")
w8 = Window.partitionBy("product_id").orderBy("quantity", "id")
w9 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w10 = Window.partitionBy("product_id").orderBy("product_id_count_quantity_filled", "id").rowsBetween(-6, -1)
w11 = Window.partitionBy("product_id").orderBy(F.col("first_known_product_id_count_quantity_filled").desc(), "id")
w12 = Window.partitionBy("product_id").orderBy(F.col("quantity_bucket_2").desc_nulls_first(), "id")

base = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-06-16").cast("date")) & (F.col("store_id") == 1))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

metrics = (
    base
    .filter(F.col("quantity_filled").between(20, 43))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_filled",
        F.ntile(10).over(w).alias("quantity_bucket"),
        F.count("quantity_filled").over(w2).alias("product_id_count_quantity_filled"),
        F.ntile(3).over(w).alias("quantity_bucket_2"),
    )
)

deduped = (
    metrics
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows = (
    deduped
    .withColumn("quantity_bucket_share", F.round((F.col("quantity_bucket") / F.sum("quantity_bucket").over(w2)), 4))
    .withColumn("last_known_quantity_bucket", F.last("quantity_bucket", ignorenulls=True).over(w4))
    .withColumn("quantity_bucket_dense_rank", F.dense_rank().over(w5))
)

with_windows_inventory = (
    with_windows
    .withColumn("first_known_product_id_count_quantity_filled", F.first("product_id_count_quantity_filled", ignorenulls=True).over(w_ordered))
    .withColumn("quantity_filled_bucket", F.ntile(5).over(w_recent))
    .withColumn("next_last_known_quantity_bucket", F.lead("last_known_quantity_bucket", 1, 0).over(w8))
)

enriched = (
    with_windows_inventory
    .select(
        "*",
        F.lead("next_last_known_quantity_bucket", 1).over(w9).alias("next_next_last_known_quantity_bucket"),
        F.avg("quantity_filled").over(w10).alias("quantity_filled_avg_6_prior"),
        F.ntile(3).over(w11).alias("first_known_product_id_count_quantity_filled_bucket"),
    )
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w12))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("activity_month"))
    .agg(
        F.sum("product_id_count_quantity_filled").alias("total_product_id_count_quantity_filled"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.percentile_approx("quantity_filled_bucket", 0.95).alias("quantity_filled_bucket_p95"),
        F.percentile_approx("quantity_bucket_2", [0.25, 0.5, 0.75]).alias("quantity_bucket_2_quartiles"),
        F.round(F.avg("first_known_product_id_count_quantity_filled_bucket"), 2).alias("avg_first_known_product_id_count_quantity_filled_bucket"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
