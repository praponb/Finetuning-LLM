from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "amount",
        "shipping_fee",
        "store_id",
        F.last_day("order_date").alias("order_date_month_end"),
    )
)
