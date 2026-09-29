from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "LOAN") & F.col("opened_date").between("2023-06-06", "2024-01-20"))
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.max("balance").alias("max_balance"),
    )
    .orderBy("last_opened_date")
)
