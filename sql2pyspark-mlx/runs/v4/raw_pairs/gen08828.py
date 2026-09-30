from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") > 0.23)
    .select(
        "id",
        "order_date",
        "shipping_fee",
        "customer_id",
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
    )
)
