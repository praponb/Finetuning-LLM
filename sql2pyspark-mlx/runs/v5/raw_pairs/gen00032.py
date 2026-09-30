from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc_nulls_first(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("amount").desc_nulls_last(), "id")
w_running = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy("amount", "id")

deduped = (
    df_order_items
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id")
)

enriched = (
    deduped
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .filter(F.col("p.customer_id").isin([16, 30, 33, 39]))
    .select(
        "s.id",
        "s.order_id",
        F.col("p.order_date"),
        F.col("p.amount"),
        F.col("p.status"),
    )
)

deduped_order_items = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "order_id", "amount")
)

with_windows = (
    deduped_order_items
    .withColumn("last4_min_amount", F.min("amount").over(w_running))
    .withColumn("last4_sum_amount", F.sum("amount").over(w_running))
    .withColumn("last4_max_amount", F.max("amount").over(w_running))
    .withColumn("last4_avg_amount", F.avg("amount").over(w_running))
    .withColumn("amount_bucket", F.ntile(3).over(w4))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("last4_max_amount") >= 1780, "LARGE").otherwise("SMALL").alias("last4_max_amount_tier"),
        F.when(F.col("amount_bucket") >= 8, "PREMIUM").otherwise("STANDARD").alias("amount_bucket_tier"),
    )
    .agg(
        F.percentile_approx("last4_min_amount", 0.95).alias("last4_min_amount_p95"),
        F.max("last4_max_amount").alias("max_last4_max_amount"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.countDistinct("order_id").alias("distinct_order_ids_2"),
        F.countDistinct("order_id").alias("distinct_order_ids_3"),
    )
    .orderBy(F.col("last4_max_amount_tier").asc_nulls_last())
)
