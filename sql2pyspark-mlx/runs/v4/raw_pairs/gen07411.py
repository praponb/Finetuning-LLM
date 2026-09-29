from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 82000, "high").when(F.col("balance") >= 9000, "mid").otherwise("low").alias("balance_bucket"),
        "status",
    )
    .agg(F.min("opened_date").alias("first_opened_date"))
)
