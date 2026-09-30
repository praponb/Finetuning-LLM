from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") > 77000)
    .groupBy(
        F.when(F.col("account_type").isin(["CREDIT", "CHECKING"]), "group_a").otherwise("other").alias("account_type_group"),
        F.when(F.col("balance") >= 67000, "high").when(F.col("balance") >= 37000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.count("*").alias("account_count"))
)
