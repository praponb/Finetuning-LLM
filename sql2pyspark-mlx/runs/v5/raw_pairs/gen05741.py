from pyspark.sql import functions as F

result = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("dim"), F.col("src.supplier_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.country").alias("country"),
        F.col("dim.city").alias("city"),
        F.col("src.category").alias("category"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("src.cost")).alias("avg_cost"),
    )
)
