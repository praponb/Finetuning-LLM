from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        "account_type",
        F.when(F.col("status").isin(["FROZEN", "OPEN"]), "tier_1").otherwise("other").alias("status_group"),
    )
    .agg(
        F.max("balance").alias("max_balance"),
        F.max("opened_date").alias("last_opened_date"),
        F.sum("balance").alias("total_balance"),
    )
    .orderBy(F.col("total_balance").desc())
)
