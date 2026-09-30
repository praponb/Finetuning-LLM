from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.max("balance").alias("max_balance"),
        F.min("balance").alias("min_balance"),
    )
    .filter(F.col("max_balance") >= 55000)
    .select("customer_id", "max_balance", "min_balance")
    .orderBy(F.col("customer_id").desc())
)
