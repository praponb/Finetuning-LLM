from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity_whole").asc_nulls_last(), "id")
w2 = Window.partitionBy("order_id").orderBy(F.col("quantity_whole").desc(), "id")
w_part = Window.partitionBy("order_id").orderBy("quantity_whole", "id").rowsBetween(-3, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("quantity") < 3)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.floor("quantity").alias("quantity_whole"),
    )
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "order_id", "product_id", "quantity_whole")
)

deduped_order_items = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "order_id", "quantity_whole")
)

metrics = (
    deduped_order_items
    .select(
        "id",
        "order_id",
        "quantity_whole",
        F.avg("quantity_whole").over(w_part).alias("last4_avg_quantity_whole"),
        F.sum("quantity_whole").over(w_part).alias("last4_sum_quantity_whole"),
        F.max("quantity_whole").over(w_part).alias("last4_max_quantity_whole"),
    )
)

enriched = (
    metrics
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.status"), F.col("b.channel"))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("last4_sum_quantity_whole") >= 390, "PREMIUM").otherwise("STANDARD").alias("last4_sum_quantity_whole_tier"),
        F.when(F.col("last4_max_quantity_whole") >= 60, "HIGH").otherwise("LOW").alias("last4_max_quantity_whole_tier"),
        "order_id",
    )
    .agg(
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
        F.count(F.when(F.col("channel") == "mobile", 1)).alias("mobile_count"),
        F.percentile_approx("last4_sum_quantity_whole", 0.95).alias("last4_sum_quantity_whole_p95"),
        F.percentile_approx("last4_sum_quantity_whole", 0.95).alias("last4_sum_quantity_whole_p95_2"),
        F.avg(F.when(F.col("status") == "PENDING", F.col("last4_sum_quantity_whole"))).alias("pending_avg_last4_sum_quantity_whole"),
        F.percentile_approx("last4_avg_quantity_whole", 0.9).alias("last4_avg_quantity_whole_p90"),
    )
    .filter(F.col("last4_avg_quantity_whole_p90") > 0)
)
