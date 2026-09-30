from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-09-03").cast("date"))
    .select("*", F.trim("account_type").alias("clean_account_type"))
)

result = (
    cleaned
    .groupBy("account_type")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.sum(F.when(F.col("status") == "FROZEN", F.col("balance")).otherwise(0)).alias("frozen_balance"),
    )
    .orderBy(F.col("account_type").desc())
)
