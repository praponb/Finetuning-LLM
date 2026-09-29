from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_order_items.alias("oi"), F.col("p.id") == F.col("oi.product_id"), "left")
    .filter(F.col("p.brand") == "Acme")
    .select(
        F.col("p.brand"),
        F.col("p.launch_date"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
    )
)
