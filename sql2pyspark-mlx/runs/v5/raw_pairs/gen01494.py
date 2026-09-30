from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "status").orderBy("running_max_amount", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("order_id", "status").orderBy("running_avg_amount", "id").rowsBetween(-2, Window.currentRow)
w_running = Window.orderBy("running_sum_amount", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.orderBy(F.col("running_sum_amount").desc_nulls_first())

valid = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-09-23 19:00:00").cast("timestamp")) & F.col("amount").between(500, 1100))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        "status",
        F.when(F.col("amount") >= 1780, "High").when(F.col("amount") >= 330, "Medium").otherwise("Low").alias("amount_band"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.paid_at",
        "src.status",
        "src.amount_band",
        F.col("p.amount"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.avg("amount").over(w).alias("running_avg_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
        F.sum("amount").over(w).alias("running_sum_amount"),
        F.count("amount").over(w).alias("running_count_amount"),
        F.first("amount", ignorenulls=True).over(w2).alias("first_known_amount"),
    )
)

enriched_payments = (
    metrics
    .filter(F.hour("paid_at").between(11, 15))
    .select(
        "id",
        "paid_at",
        "running_max_amount",
        "running_sum_amount",
        "running_count_amount",
        "first_known_amount",
        F.avg("running_max_amount").over(w3).alias("running_max_amount_avg_3_prior"),
        F.avg("running_avg_amount").over(w4).alias("running_avg_amount_avg_2"),
    )
)

metrics_payments = (
    enriched_payments
    .withColumn("last3_avg_running_count_amount", F.avg("running_count_amount").over(w_running))
    .withColumn("last3_count_running_count_amount", F.count("running_count_amount").over(w_running))
    .withColumn("last3_min_running_count_amount", F.min("running_count_amount").over(w_running))
    .withColumn("last3_sum_running_count_amount", F.sum("running_count_amount").over(w_running))
    .withColumn("running_sum_amount_percent_rank", F.percent_rank().over(w6))
)

result = (
    metrics_payments
    .select(
        "id",
        "paid_at",
        "running_max_amount",
        "running_sum_amount",
        "running_count_amount",
        "first_known_amount",
        "running_max_amount_avg_3_prior",
        "last3_count_running_count_amount",
        "last3_sum_running_count_amount",
    )
    .orderBy(
        F.col("running_max_amount_avg_3_prior").desc_nulls_last(),
        F.col("running_sum_amount").desc(),
    )
)
