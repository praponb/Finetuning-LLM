from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(((F.col("opened_date") >= "2025-05-01") & (F.col("balance") >= 34000)) | (F.col("opened_date") < "2024-11-15"))
    .select(
        "id",
        "customer_id",
        "status",
        "opened_date",
        F.lower("account_type").alias("account_type_lower"),
    )
)
