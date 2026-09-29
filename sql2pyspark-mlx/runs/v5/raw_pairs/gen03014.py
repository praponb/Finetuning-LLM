from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", "store_id")
    .agg(
        F.percentile_approx("revenue", 0.5).alias("revenue_p50"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("units", 0.95).alias("units_p95"),
    )
)
