from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .groupBy(F.col("p.category"))
    .agg(
        F.count("*").alias("num_store_sales"),
        F.round(F.avg(F.col("ss.units")), 2).alias("avg_units"),
    )
    .orderBy(F.col("avg_units").desc(), "category")
    .limit(10)
)
