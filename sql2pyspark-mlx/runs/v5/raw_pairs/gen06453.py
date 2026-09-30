from pyspark.sql import functions as F

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") <= 310))
    .withColumn("greatest_quantity_unit_price", F.greatest("quantity", "unit_price"))
    .withColumn("greatest_quantity_unit_price_2", F.greatest("quantity", "unit_price"))
)

result = (
    cleaned
    .select("product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("quantity").between(5, 8))
        .select("product_id")
    )
)
