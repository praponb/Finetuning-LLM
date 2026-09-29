from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2]))
    .groupBy(
        F.when(F.col("units") >= 42, "high").when(F.col("units") >= 19, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.max("sale_date").alias("last_sale_date"),
    )
)
