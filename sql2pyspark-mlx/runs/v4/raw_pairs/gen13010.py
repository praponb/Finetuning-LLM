from pyspark.sql import functions as F

result = (
    df_store_sales
    .rollup(
        F.when(F.col("units") >= 33, "LARGE").otherwise("SMALL").alias("units_tier"),
    )
    .agg(
        F.percentile_approx("units", 0.95).alias("units_p95"),
        F.sum("units").alias("total_units"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.stddev_samp("units").alias("units_stddev"),
        F.stddev_samp("units").alias("units_stddev_2"),
    )
    .orderBy(F.col("units_p95").desc())
)
