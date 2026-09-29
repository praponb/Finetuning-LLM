from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("status").isin(["CANCELLED", "SHIPPED", "PENDING"]) & ((F.col("status") == "CANCELLED") | F.col("status").isin(["COMPLETED", "SHIPPED", "PENDING"])))
    .select(
        "id",
        "status",
        "store_id",
        "customer_id",
        F.when(F.col("shipping_fee") >= 35, "High").when(F.col("shipping_fee") >= 10, "Medium").otherwise("Low").alias("shipping_fee_level"),
    )
)
