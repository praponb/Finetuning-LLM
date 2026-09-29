from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") >= 3)
    .select("product_id", "avg_quantity", "quantity_count")
    .orderBy("avg_quantity")
)
