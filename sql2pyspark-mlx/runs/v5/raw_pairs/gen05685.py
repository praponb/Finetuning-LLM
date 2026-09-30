from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select(
        "id",
        "customer_id",
        F.round((F.col("shipping_fee") * 1.1), 2).alias("shipping_fee_scaled"),
    )
    .orderBy(F.col("shipping_fee_scaled").desc(), "id")
    .limit(5)
)
