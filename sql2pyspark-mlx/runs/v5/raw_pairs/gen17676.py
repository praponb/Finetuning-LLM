from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~(F.hour("paid_at").between(10, 17) & (F.col("status").isin(["SUCCESS", "REFUNDED"]) | (F.to_date("paid_at") < "2023-04-24"))))
    .groupBy(
        F.when(F.col("amount") >= 1630, "high").when(F.col("amount") >= 760, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.count("*").alias("payment_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)
