from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CREDIT", "SAVINGS"]))
    .select(
        "id",
        "account_type",
        F.when(F.col("status").isin(["OPEN", "CLOSED"]), "Tier 1").otherwise("Other").alias("status_group"),
    )
)
