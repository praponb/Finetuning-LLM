from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") > 900) & (F.col("order_id") == 39))
    .groupBy("status", F.month("paid_at").alias("paid_month"))
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy("status")
)
