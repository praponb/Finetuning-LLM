from pyspark.sql import functions as F

base = df_order_items.filter((F.col("order_id") == 38) & (F.col("quantity") < 3))

product_id_summary = (
    base
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.count("quantity").alias("quantity_count"),
    )
)

result = product_id_summary.select("product_id", "quantity_range", "quantity_count")
