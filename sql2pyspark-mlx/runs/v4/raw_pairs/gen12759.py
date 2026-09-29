from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("*").alias("event_count"),
        F.percentile_approx("units", 0.5).alias("units_p50"),
        F.min("units").alias("min_units"),
    )
    .orderBy(F.col("event_count").asc_nulls_last())
)
