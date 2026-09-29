from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("oi.product_id") == F.col("p.id"), "left")
    .filter(F.col("oi.quantity") == 4)
    .select(
        F.col("p.cost"),
        F.col("p.launch_date"),
        F.col("p.brand"),
        F.col("oi.product_id"),
        (F.col("oi.unit_price") + F.col("oi.quantity")).alias("line_total"),
    )
    .orderBy(F.col("brand").desc())
)
