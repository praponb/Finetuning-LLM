from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-05-21").cast("date"))
    .withColumn("revenue_filled", F.coalesce("revenue", F.lit(0)))
)

result = (
    cleaned
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("a.revenue_filled")).alias("avg_revenue_filled"),
    )
)
