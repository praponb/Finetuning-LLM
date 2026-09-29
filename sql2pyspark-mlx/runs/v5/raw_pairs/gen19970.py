from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("quantity_rounded", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity_rounded").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("quantity_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("quantity_rounded").desc_nulls_first())
w5 = Window.orderBy("quantity_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("product_id").orderBy("seq_no", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.orderBy(F.col("seq_no").desc(), "id")
w8 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("product_id").orderBy("running_quantity_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("product_id").orderBy(F.col("seq_no").desc(), "id")
w_recent = Window.orderBy("seq_no_avg_5", "id").rowsBetween(-2, Window.currentRow)

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") <= 400))
    .select("id", "product_id", F.round("quantity", 2).alias("quantity_rounded"))
)

with_windows = (
    filtered
    .filter(F.col("product_id").isin([4, 21, 32, 38]))
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.ntile(3).over(w2).alias("quantity_rounded_bucket"),
        F.sum("quantity_rounded").over(w3).alias("running_quantity_rounded"),
        F.rank().over(w4).alias("quantity_rounded_rank"),
        F.sum("quantity_rounded").over(w5).alias("running_quantity_rounded_2"),
    )
)

enriched = (
    with_windows
    .withColumn("seq_no_avg_5", F.avg("seq_no").over(w_part))
    .withColumn("seq_no_bucket", F.ntile(3).over(w7))
    .withColumn("last_known_seq_no", F.last("seq_no", ignorenulls=True).over(w8))
    .withColumn("running_running_quantity_rounded_2", F.sum("running_quantity_rounded_2").over(w9))
    .withColumn("seq_no_2", F.row_number().over(w_grp))
)

filtered_order_items = (
    enriched
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "quantity_rounded",
        "seq_no",
        "quantity_rounded_bucket",
        "running_quantity_rounded",
        "quantity_rounded_rank",
        "running_quantity_rounded_2",
        "seq_no_avg_5",
        "seq_no_bucket",
        "last_known_seq_no",
        "seq_no_2",
        F.round((F.col("seq_no_bucket") * 0.85), 2).alias("seq_no_bucket_adjusted"),
        F.when(F.col("quantity_rounded_rank") >= 8, "High").when(F.col("quantity_rounded_rank") >= 3, "Medium").otherwise("Low").alias("quantity_rounded_rank_level"),
    )
)

metrics = (
    filtered_order_items
    .withColumn("last3_avg_seq_no_bucket", F.avg("seq_no_bucket").over(w_recent))
    .withColumn("last3_min_seq_no_bucket", F.min("seq_no_bucket").over(w_recent))
    .withColumn("last3_count_seq_no_bucket", F.count("seq_no_bucket").over(w_recent))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("quantity_rounded_bucket") >= 8, "HIGH").otherwise("LOW").alias("quantity_rounded_bucket_tier"),
        F.when(F.col("quantity_rounded") >= 79, "LARGE").otherwise("SMALL").alias("quantity_rounded_tier"),
        F.when(F.col("seq_no_avg_5") >= 8, "PREMIUM").otherwise("STANDARD").alias("seq_no_avg_5_tier"),
    )
    .agg(
        F.percentile_approx("last3_count_seq_no_bucket", 0.5).alias("last3_count_seq_no_bucket_p50"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("running_quantity_rounded_2").alias("min_running_quantity_rounded_2"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.countDistinct("id").alias("distinct_ids_3"),
    )
    .filter(F.col("min_running_quantity_rounded_2") < 1000000)
    .orderBy(F.col("seq_no_avg_5_tier").asc_nulls_first())
)
