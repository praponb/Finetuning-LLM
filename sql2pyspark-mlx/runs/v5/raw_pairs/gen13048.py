from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.16, 0.39) & (F.col("shipping_fee") < 29))
    .select(
        "id",
        "channel",
        "shipping_fee",
        "order_date",
        F.col("shipping_fee").cast("int").alias("shipping_fee_int"),
    )
)
