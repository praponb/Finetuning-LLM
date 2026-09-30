from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("opened_date") >= "2024-01-20"))
    .groupBy("status")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.sum("balance").alias("total_balance"),
    )
    .filter(F.col("total_balance") >= 358000)
    .orderBy(F.col("avg_balance").desc(), F.col("total_balance").desc())
)
