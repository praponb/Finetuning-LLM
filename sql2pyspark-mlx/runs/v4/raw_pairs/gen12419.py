from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~(((F.col("status") == "FROZEN") & (F.col("opened_date") <= "2024-07-01")) | F.col("customer_id").isin([11, 28, 36])))
    .select(
        "id",
        "customer_id",
        "balance",
        "account_type",
        F.lower("account_type").alias("account_type_lower"),
    )
)
