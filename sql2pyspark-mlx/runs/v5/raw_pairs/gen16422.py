from pyspark.sql import functions as F

rollup_totals = (
    df_store_sales
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("a.units")), 2).alias("avg_units"),
    )
)

result = (
    rollup_totals
    .select("category", "cnt", "avg_units")
    .orderBy("avg_units", F.col("category").desc())
)
