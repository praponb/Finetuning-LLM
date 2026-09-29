from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") <= 15)
    .groupBy(
        F.when(F.col("units") >= 40, "high").when(F.col("units") >= 19, "mid").otherwise("low").alias("units_bucket"),
        F.year("sale_date").alias("sale_year"),
    )
    .agg(
        F.min("units").alias("min_units"),
        F.min("revenue").alias("min_revenue"),
        F.count("*").alias("total_count"),
    )
)
