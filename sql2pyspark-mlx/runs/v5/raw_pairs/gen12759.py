from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-05-08")))
    .select("*", F.date_trunc("WEEK", "sale_date").alias("sale_date_period"))
)

result = (
    base
    .groupBy(
        "product_id",
        F.when(F.col("units") >= 30, "PREMIUM").otherwise("STANDARD").alias("units_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max("sale_date_period").alias("last_seen"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.count("*").alias("num_rows"),
    )
)
