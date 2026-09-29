from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([7, 8, 11]))
    .groupBy(
        "product_id",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .filter(F.col("avg_quantity") < 54)
    .orderBy("product_id")
)
