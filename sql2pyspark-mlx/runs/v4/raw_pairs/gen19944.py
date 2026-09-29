from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([2, 5, 9]))
    .groupBy(
        "status",
        F.when(F.col("balance") >= 59000, "high").when(F.col("balance") >= 35000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.sum("balance").alias("total_balance"),
        F.max("balance").alias("max_balance"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 3)
    .drop("cnt")
    .orderBy("balance_bucket", "status")
    .limit(5)
)
