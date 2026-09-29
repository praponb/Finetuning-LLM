from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["LOAN", "SAVINGS"]))
    .select(
        "account_type",
        "opened_date",
        F.trim("account_type").alias("clean_account_type"),
        F.concat_ws("-", "account_type", "status").alias("account_type_status_key"),
    )
    .orderBy("account_type_status_key", "account_type")
)
