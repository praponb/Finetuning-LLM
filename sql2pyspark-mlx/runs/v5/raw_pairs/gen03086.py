from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("e.quantity")), 2).alias("total_quantity"),
    )
)
