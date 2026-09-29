from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") >= 32000)
    .select(
        "account_type",
        F.date_sub("opened_date", 57).alias("opened_date_minus_57d"),
    )
)
