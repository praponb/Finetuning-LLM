from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(F.col("amount").isNotNull() | ~F.col("method").isin(["cash", "card", "paypal"]))
)

payments_stats = (
    recent_payments
    .groupBy("status")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = payments_stats.select("status", "avg_amount", "status_values")
