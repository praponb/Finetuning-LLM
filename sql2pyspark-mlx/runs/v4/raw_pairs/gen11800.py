from pyspark.sql import functions as F

filtered_order_items = df_order_items.filter(F.col("order_id").isin([6, 21, 24, 31]))

order_id_summary = (
    filtered_order_items
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
    )
)

result = (
    order_id_summary
    .filter(F.col("avg_quantity") < 4.1)
    .orderBy("order_id")
)
