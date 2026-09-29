from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "SAVINGS", "CREDIT"]))
    .withColumn("status_lower", F.lower("status"))
)

accounts_stats = (
    filtered_accounts
    .groupBy("account_type")
    .agg(
        F.count("balance").alias("balance_count"),
        F.countDistinct("balance").alias("unique_statuss"),
    )
)

result = (
    accounts_stats
    .select("account_type", "balance_count")
    .orderBy("balance_count")
)
