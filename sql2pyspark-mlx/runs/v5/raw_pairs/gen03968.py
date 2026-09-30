from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "status",
        "order_date",
        "shipping_fee",
        F.round("shipping_fee", 2).alias("shipping_fee_rounded"),
    )
)
