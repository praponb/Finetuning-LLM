from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 8, 9]))
    .groupBy("product_id")
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.min("units").alias("min_units"),
    )
    .filter(F.col("avg_units") >= 65)
    .select("product_id", "avg_units", "min_units")
)
