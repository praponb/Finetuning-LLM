from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") != "CREDIT") | (F.col("account_type") != "SAVINGS"))
    .groupBy(
        F.when(F.col("balance") >= 87000, "high").when(F.col("balance") >= 12000, "mid").otherwise("low").alias("balance_bucket"),
        "status",
    )
    .agg(F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"))
    .filter(F.col("loan_count") > 1)
    .orderBy(F.col("status").desc(), "loan_count")
)
