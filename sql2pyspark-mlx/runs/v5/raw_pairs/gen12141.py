from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 32)
    .select(
        "id",
        "account_type",
        F.lower("status").alias("status_lower"),
        F.round((F.col("balance") * 0.85), 2).alias("adjusted_balance"),
    )
    .orderBy(F.col("status_lower").desc())
)
