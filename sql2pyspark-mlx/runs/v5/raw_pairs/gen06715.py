from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~(F.col("unit_price").isNotNull() & F.col("order_id").isin([12, 20, 33])))
    .select(
        "quantity",
        "unit_price",
        "order_id",
        F.when(F.col("unit_price") >= 440, "High").when(F.col("unit_price") >= 100, "Medium").otherwise("Low").alias("unit_price_level"),
        (F.col("unit_price") - F.col("quantity")).alias("line_total"),
    )
    .orderBy(F.col("order_id").desc())
)
