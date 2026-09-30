from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "LOAN") & (F.col("balance") > 75000))
    .select(
        "id",
        "status",
        "customer_id",
        "account_type",
        F.concat_ws("/", "account_type", "status").alias("account_type_status_key"),
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
    )
)
