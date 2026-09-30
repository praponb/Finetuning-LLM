from pyspark.sql import functions as F

product_totals = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("last_restock").alias("latest_last_restock"),
    )
)

result = (
    df_products.alias("p")
    .join(product_totals.alias("P"), F.col("p.id") == F.col("P.product_id"), "left")
    .select(
        F.col("p.id"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("P.quantity_count"),
        F.col("P.latest_last_restock"),
    )
    .orderBy("latest_last_restock")
)
