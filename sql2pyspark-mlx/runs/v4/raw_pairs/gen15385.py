from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status", "account_type")
    .agg(
        F.max("balance").alias("max_balance"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .filter(F.col("max_balance") < 38000)
    .orderBy("status", F.col("max_balance").desc())
)
