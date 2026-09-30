from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.countDistinct("balance").alias("unique_customers"),
        F.avg("balance").alias("avg_balance"),
    )
    .orderBy(F.col("avg_balance").desc())
)
