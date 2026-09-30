from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter((F.col("balance") > 28000) & ((F.col("opened_date") < "2023-06-01") | (F.col("account_type") == "CREDIT")))
    .withColumn("status_lower", F.lower("status"))
)

summary = (
    recent_accounts
    .groupBy(
        F.when(F.col("balance") >= 86000, "high").when(F.col("balance") >= 23000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
    )
)

result = (
    summary
    .filter(F.col("pct_open") >= 55)
    .orderBy("pct_open")
)
