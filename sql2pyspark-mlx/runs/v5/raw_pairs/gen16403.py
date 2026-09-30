from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(25000, 38000))
    .groupBy("customer_id", "status")
    .agg(
        F.min("balance").alias("min_balance"),
        F.avg("balance").alias("avg_balance"),
    )
    .orderBy(F.col("min_balance").desc(), "avg_balance")
)
