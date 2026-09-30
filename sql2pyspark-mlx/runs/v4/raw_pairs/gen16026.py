from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.to_date("txn_time") < "2023-04-01") & F.col("account_id").isin([3, 32, 36]))
    .groupBy(
        "is_flagged",
        F.when(F.col("merchant").isin(["Grab", "Starbucks"]), "tier_1").otherwise("other").alias("merchant_group"),
    )
    .agg(F.countDistinct("amount").alias("unique_accounts"))
    .filter(F.col("unique_accounts") >= 3)
)
