from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").isNotNull())
    .select(
        "id",
        "customer_id",
        F.greatest("shipping_fee", "discount").alias("greatest_shipping_fee_discount"),
    )
)
