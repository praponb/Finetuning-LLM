from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(32, 39) & F.col("customer_id").isNull())
    .select(
        "shipping_fee",
        "order_date",
        (F.col("discount") - F.col("shipping_fee")).alias("discount_minus_shipping_fee"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
