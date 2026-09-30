from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").isNotNull() & (F.col("discount").between(0.11, 0.39) | F.month("order_date").isin([8, 11, 12])))
    .select(
        "shipping_fee",
        "discount",
        F.when(F.col("discount") >= 0.3, "High").when(F.col("discount") >= 0.08, "Medium").otherwise("Low").alias("discount_tier"),
    )
    .orderBy("shipping_fee", "discount_tier")
)
