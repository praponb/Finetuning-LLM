from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") >= 56000) & (F.col("account_type") == "LOAN"))
    .select(
        "id",
        "opened_date",
        "balance",
        F.dayofweek("opened_date").alias("opened_date_dayofweek"),
    )
)
