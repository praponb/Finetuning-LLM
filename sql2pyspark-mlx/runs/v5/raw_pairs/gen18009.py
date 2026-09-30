from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        F.add_months("order_date", 6).alias("order_date_plus_months"),
        F.concat_ws("-", "channel", "status").alias("channel_status_key"),
    )
)
