from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~((F.col("customer_id").isin([8, 22, 26, 38]) & (F.col("opened_date") <= "2024-11-01")) | F.col("customer_id").isin([27, 36])))
    .groupBy(
        F.when(F.col("status").isin(["CLOSED", "OPEN"]), "tier_1").otherwise("other").alias("status_group"),
        F.when(F.col("account_type").isin(["SAVINGS", "CHECKING"]), "tier_1").otherwise("other").alias("account_type_group"),
        "status",
    )
    .agg(
        F.sum(F.when(F.col("account_type") == "CHECKING", 1).otherwise(0)).alias("checking_count"),
    )
)
