from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~((F.col("status") == "OPEN") & (F.col("customer_id") == 29) & F.col("status").isin(["CLOSED", "FROZEN"])))
    .select(
        "id",
        "status",
        "account_type",
        F.round((F.col("balance") * 0.85), 2).alias("adjusted_balance"),
    )
)
