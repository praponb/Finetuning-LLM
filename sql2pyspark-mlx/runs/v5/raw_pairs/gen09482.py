from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "balance",
        F.concat("status", F.lit(" "), "account_type").alias("status_account_type"),
        F.month("opened_date").alias("opened_date_month"),
    )
    .orderBy("opened_date_month", F.col("balance").desc())
)
