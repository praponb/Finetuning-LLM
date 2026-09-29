from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("p.launch_date")).alias("launch_month"))
    .agg(
        F.max(F.col("oi.quantity")).alias("max_quantity"),
        F.count("*").alias("num_order_items"),
    )
    .filter(F.col("num_order_items") > 4)
)
