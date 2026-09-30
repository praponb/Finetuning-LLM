from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "SAVINGS"]) | F.col("customer_id").isNotNull())
    .groupBy(
        F.when(F.col("balance") >= 74000, "high").when(F.col("balance") >= 28000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.min("opened_date").alias("min_opened_date"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
    .filter(F.col("stddev_balance") >= 33)
    .orderBy("min_opened_date")
)
