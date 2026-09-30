from pyspark.sql import functions as F

result = (
    df_store_sales
    .rollup("product_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("units", 0.5).alias("units_p50"),
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
    )
)
