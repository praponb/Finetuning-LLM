from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 6, 8]))
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("sale_month"))
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.min("revenue").alias("min_revenue"),
        F.count("units").alias("units_count"),
        F.sum("units").alias("total_units"),
    )
    .filter(F.col("total_units") > 90)
    .drop("total_units")
    .orderBy(F.col("min_revenue").desc())
)
