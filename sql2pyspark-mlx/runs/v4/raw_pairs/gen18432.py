from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "COMPLETED") & F.col("shipping_fee").between(12, 25))
    .select(
        "customer_id",
        F.least("discount", "shipping_fee").alias("least_discount_shipping_fee"),
    )
    .orderBy(F.col("least_discount_shipping_fee").desc())
)
