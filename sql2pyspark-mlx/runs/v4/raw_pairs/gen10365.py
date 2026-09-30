from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "CLOSED"]) & (F.col("account_type") == "LOAN"))
    .select(
        "id",
        "status",
        "customer_id",
        "balance",
        F.lower("status").alias("status_lower"),
    )
)
