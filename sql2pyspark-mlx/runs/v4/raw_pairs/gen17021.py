from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "CLOSED"]) & F.col("customer_id").isNull())
    .groupBy(
        F.when(F.col("account_type").isin(["LOAN", "CREDIT"]), "tier_1").otherwise("other").alias("account_type_group"),
        "status",
    )
    .agg(F.count("balance").alias("balance_count"))
    .orderBy("balance_count", "status", "account_type_group")
    .limit(1)
)
