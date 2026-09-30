from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.col("order_date").alias("event_date"))
    .unionByName(
        df_accounts
        .filter(F.col("account_type") == "SAVINGS")
        .select(F.col("opened_date").alias("event_date"))
    )
)
