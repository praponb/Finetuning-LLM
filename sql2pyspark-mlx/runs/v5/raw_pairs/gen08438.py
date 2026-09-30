from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date") >= "2023-06-20") & (F.year("opened_date") >= 2023))
    .select(
        "id",
        "customer_id",
        F.concat("account_type", F.lit(" "), "status").alias("account_type_status"),
    )
)
