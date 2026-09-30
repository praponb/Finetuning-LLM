from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.avg(F.col("s.quantity")), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").asc_nulls_last())
)
