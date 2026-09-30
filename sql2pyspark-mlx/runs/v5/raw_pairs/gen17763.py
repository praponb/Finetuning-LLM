from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(36000, 46000))
    .select(
        "opened_date",
        F.concat_ws(",", "account_type", "status").alias("account_type_status_key"),
        F.dayofweek("opened_date").alias("opened_date_dayofweek"),
    )
    .orderBy("account_type_status_key")
)
