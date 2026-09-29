from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock_period", "id")
w2 = Window.partitionBy("product_id").orderBy("last_restock_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "store_id").orderBy("is_new_burst", "id")
w_running = Window.orderBy(F.col("is_new_burst").desc_nulls_last())
w5 = Window.partitionBy("store_id").orderBy("burst_id", "id")
w6 = Window.partitionBy("store_id").orderBy(F.col("is_new_burst_bucket").asc_nulls_first(), "id")
w7 = Window.partitionBy("store_id").orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w8 = Window.partitionBy("store_id").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("running_burst_id").orderBy(F.col("avg_cumulative_is_new_burst_bucket").desc())

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2024-04-01").cast("date"))
    .select(
        "*",
        F.date_trunc("year", "last_restock").alias("last_restock_period"),
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
    )
)

gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("last_restock_period").over(w).isNull() | (F.col("last_restock_period") > F.lag("last_restock_period").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

scored = (
    sessionized
    .select(
        "*",
        F.ntile(4).over(w3).alias("is_new_burst_bucket"),
        F.cume_dist().over(w_running).alias("is_new_burst_cume_dist"),
    )
)

ranked = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

deduped = (
    ranked
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.avg("is_new_burst").over(w7).alias("is_new_burst_avg_7d"),
        F.sum("quantity").over(w8).alias("cumulative_quantity"),
        F.sum("burst_id").over(w9).alias("running_burst_id"),
        F.sum("is_new_burst_bucket").over(w10).alias("cumulative_is_new_burst_bucket"),
    )
)

result = (
    metrics
    .groupBy(
        "running_burst_id",
        F.when(F.col("cumulative_quantity") >= 1123, "PREMIUM").otherwise("STANDARD").alias("cumulative_quantity_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg("cumulative_is_new_burst_bucket").alias("avg_cumulative_is_new_burst_bucket"),
        F.percentile_approx("running_burst_id", 0.95).alias("running_burst_id_p95"),
    )
    .withColumn("avg_cumulative_is_new_burst_bucket_dense_rank", F.dense_rank().over(w11))
    .orderBy(F.col("cumulative_quantity_tier").desc_nulls_last())
)
