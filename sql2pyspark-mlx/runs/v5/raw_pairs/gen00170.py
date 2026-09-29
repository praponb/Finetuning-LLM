from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "discount",
        "order_date",
        "channel",
        F.when(F.col("discount") >= 0.35, "High").when(F.col("discount") >= 0.1, "Medium").otherwise("Low").alias("discount_level"),
    )
    .orderBy("discount", F.col("id").desc())
    .limit(10)
)
