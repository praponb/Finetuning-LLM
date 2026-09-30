from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        "status",
        F.when(F.col("status").isin(["FAILED", "REFUNDED"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .filter(F.col("avg_amount") > 31)
    .select("status", "status_group", "avg_amount", "first_paid_at")
    .orderBy("first_paid_at", "status_group")
)
