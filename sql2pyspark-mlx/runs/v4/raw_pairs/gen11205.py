from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.date_trunc("week", "sale_date").alias("week_start"),
        F.when(F.col("revenue") >= 3800, "HIGH").otherwise("LOW").alias("revenue_tier"),
        F.when(F.col("units") >= 31, "PREMIUM").otherwise("STANDARD").alias("units_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("units", 0.9).alias("units_p90"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.percentile_approx("revenue", 0.95).alias("revenue_p95"),
    )
    .orderBy("units_p90")
)
