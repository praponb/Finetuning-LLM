from pyspark.sql import functions as F

base = df_order_items.filter((F.col("order_id") == 10) & F.col("quantity").between(7, 8))

product_id_summary = (
    base
    .groupBy("product_id", "order_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.count("*").alias("cnt"),
    )
)

result = product_id_summary.orderBy(F.col("cnt").desc())
