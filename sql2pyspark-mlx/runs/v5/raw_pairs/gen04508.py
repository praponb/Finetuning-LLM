from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3]))
    .groupBy("product_id")
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.count("units").alias("units_count"),
        F.sum("units").alias("total_units"),
    )
    .filter(F.col("total_units") > 140)
    .drop("total_units")
)
