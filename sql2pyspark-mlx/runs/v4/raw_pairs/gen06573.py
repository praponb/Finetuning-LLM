from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("activity_month"))
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.round(F.avg("units"), 2).alias("avg_units_2"),
        F.max("units").alias("max_units"),
        F.count("*").alias("event_count"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
    )
    .filter(F.col("event_count") > 1)
    .orderBy(F.col("max_units").desc_nulls_last())
)
