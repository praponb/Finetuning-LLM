from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "opened_date",
        F.concat("account_type", F.lit("/"), "status").alias("account_type_status"),
        F.length("account_type").alias("account_type_length"),
    )
)
