from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "LOAN"]) | F.col("customer_id").isin([6, 13, 24, 39]))
    .select(
        "id",
        "status",
        "balance",
        "customer_id",
        F.when(F.col("status").isin(["OPEN", "CLOSED"]), "Tier 1").otherwise("Other").alias("status_group"),
    )
)
