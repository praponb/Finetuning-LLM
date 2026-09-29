from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 77000, "high").when(F.col("balance") >= 16000, "mid").otherwise("low").alias("balance_bucket"),
        "account_type",
        "status",
    )
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
    .orderBy("balance_bucket", "status")
)
