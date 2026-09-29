from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-05-07").cast("date"))
    .withColumn("revenue_per_unit", (F.col("units") * F.col("revenue")))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("revenue_per_unit") >= 88, "LARGE").otherwise("SMALL").alias("revenue_per_unit_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("revenue", 0.5).alias("revenue_p50"),
        F.min("units").alias("min_units"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.bit_or("units").alias("units_bit_or"),
    )
)
