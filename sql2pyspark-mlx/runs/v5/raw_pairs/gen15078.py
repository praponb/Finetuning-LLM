from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 1350) & (F.to_date("paid_at") < "2023-06-01"))
    .groupBy(
        "status",
        "method",
        F.when(F.col("amount") >= 1720, "high").when(F.col("amount") >= 770, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.count(F.when(F.col("method") == "card", 1)).alias("card_count"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc(), "amount_bucket")
)
