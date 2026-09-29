from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "channel",
        F.when(F.col("amount") >= 1370, "High").when(F.col("amount") >= 360, "Medium").otherwise("Low").alias("amount_tier"),
        (F.col("discount") - F.col("amount")).alias("discount_minus_amount"),
    )
    .orderBy("channel")
)
