from pyspark.sql import functions as F

dim_totals = (
    df_inventory
    .alias("e")
    .join(df_products.alias("dim"), F.col("e.product_id") == F.col("dim.id"), "inner")
    .groupBy(F.col("dim.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("e.quantity")).alias("total_quantity"),
    )
)

result = dim_totals.select("category", "cnt", "total_quantity")
