from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([2, 5, 40]))
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .orderBy("quantity_range")
)
