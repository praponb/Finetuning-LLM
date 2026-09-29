from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        (F.max("balance") - F.min("balance")).alias("balance_range"),
        F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
    )
    .filter(F.col("closed_count") > 28)
    .select("account_type", "balance_range", "closed_count")
    .orderBy(
        F.col("balance_range").desc(),
        F.col("closed_count").desc(),
        "account_type",
    )
    .limit(50)
)
