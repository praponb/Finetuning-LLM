from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_trunc("MONTH", "sale_date").alias("activity_month"))
    .agg(
        F.bit_and("units").alias("units_bit_and"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.count("*").alias("event_count"),
        F.percentile_approx("units", 0.5).alias("units_p50"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
    .orderBy(F.col("corr_revenue_units").asc_nulls_first())
)
