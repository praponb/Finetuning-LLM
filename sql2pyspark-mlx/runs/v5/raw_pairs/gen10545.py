from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type") == "SAVINGS")
    .select(
        "opened_date",
        "status",
        "balance",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.upper("account_type").alias("account_type_upper"),
    )
)
