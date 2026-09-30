from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 14)
    .select(
        "order_date",
        "customer_id",
        F.col("shipping_fee").cast("int").alias("shipping_fee_int"),
        F.trim("status").alias("clean_status"),
    )
)
