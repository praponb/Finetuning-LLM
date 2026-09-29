from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "outer")
    .groupBy(
        F.date_trunc("MONTH", F.col("p.launch_date")).alias("launch_month"),
        F.col("p.brand"),
    )
    .agg(
        F.round(F.avg(F.col("i.quantity")), 2).alias("avg_quantity"),
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
    )
    .orderBy("quantity_range", F.col("avg_quantity").desc())
)
