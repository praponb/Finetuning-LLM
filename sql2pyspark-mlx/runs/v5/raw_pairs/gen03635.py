from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "store_id",
        "shipping_fee",
        "discount",
        F.when(F.col("discount") >= 0.44, "High").when(F.col("discount") >= 0.17, "Medium").otherwise("Low").alias("discount_level"),
    )
)
