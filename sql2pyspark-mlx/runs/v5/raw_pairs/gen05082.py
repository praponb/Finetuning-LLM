from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        F.date_add("opened_date", 60).alias("opened_date_plus_60d"),
    )
    .orderBy("id")
)
