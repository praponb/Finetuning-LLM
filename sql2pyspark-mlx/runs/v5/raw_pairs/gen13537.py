from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "customer_id",
        (F.col("shipping_fee") + F.col("amount")).alias("amount_with_shipping"),
    )
    .orderBy(F.col("amount_with_shipping").desc())
)
