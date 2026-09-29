from pyspark.sql import functions as F

filtered = (
    df_order_items
    .filter(F.col("quantity") <= 8)
    .withColumn("line_total", (F.col("quantity") + F.col("unit_price")))
    .withColumn("least_unit_price_quantity", F.least("unit_price", "quantity"))
)

result = (
    filtered
    .groupBy(
        "product_id",
        F.when(F.col("least_unit_price_quantity") >= 78, "LARGE").otherwise("SMALL").alias("least_unit_price_quantity_tier"),
    )
    .agg(
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.max("quantity").alias("max_quantity"),
        F.min("line_total").alias("min_line_total"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("quantity_bit_or") < 1)
    .orderBy("corr_quantity_unit_price")
)
