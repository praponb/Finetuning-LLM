from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        F.date_format("opened_date", "yyyy-MM").alias("opened_date_label"),
    )
    .orderBy("opened_date_label")
)
