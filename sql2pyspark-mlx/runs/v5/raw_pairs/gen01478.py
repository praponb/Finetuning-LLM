from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") != "OPEN")
    .select(
        "balance",
        F.concat_ws("-", "status", "account_type").alias("status_account_type_key"),
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
    .orderBy("balance")
)
