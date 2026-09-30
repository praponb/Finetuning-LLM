from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") < "2023-05-01") & (F.col("balance") < 59000) & (F.col("opened_date") <= "2023-07-01"))
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "SAVINGS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_savings"),
        F.max("opened_date").alias("latest_opened_date"),
    )
    .orderBy(F.col("pct_savings").desc())
)
