from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.year("opened_date") >= 2024)
    .select(
        "balance",
        "account_type",
        F.date_sub("opened_date", 30).alias("opened_date_minus_30d"),
    )
)
