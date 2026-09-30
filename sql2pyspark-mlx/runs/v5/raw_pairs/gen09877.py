from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") > 70)
    .select("product_id", "quantity_range", "avg_quantity")
)
