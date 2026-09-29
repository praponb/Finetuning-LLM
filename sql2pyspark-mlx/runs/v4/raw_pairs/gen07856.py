from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("customer_id") == 36) | (F.col("status") == "FROZEN"))
    .groupBy(
        "status",
        F.when(F.col("status").isin(["FROZEN", "OPEN"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        F.count("*").alias("account_count"),
        F.max("balance").alias("max_balance"),
    )
    .filter(F.col("max_balance") >= 55000)
)
