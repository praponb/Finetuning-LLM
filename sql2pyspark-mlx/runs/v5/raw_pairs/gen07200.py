from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.year("opened_date") >= 2025) & F.col("customer_id").isin([7, 13, 19, 24]))
    .select(
        "id",
        "balance",
        F.concat_ws("/", "status", "account_type").alias("status_account_type_key"),
    )
)
