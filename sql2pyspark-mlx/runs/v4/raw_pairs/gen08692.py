from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") == "SHIPPED")
    .select(
        "id",
        "amount",
        F.least("shipping_fee", "amount").alias("least_shipping_fee_amount"),
        F.round("amount", 1).alias("amount_rounded"),
    )
    .orderBy("amount_rounded", "id")
    .limit(10)
)
