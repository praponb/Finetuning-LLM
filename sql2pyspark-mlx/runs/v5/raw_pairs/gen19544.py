from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") < 120) & F.col("unit_price").between(120, 330))
    .agg(
        F.sum("quantity").alias("total_quantity"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
    .orderBy(F.col("total_quantity").desc())
)
