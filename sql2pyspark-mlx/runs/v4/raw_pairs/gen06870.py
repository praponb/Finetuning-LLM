from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-08-21").cast("date"))
    .withColumn("adjusted_units", F.round((F.col("units") * 100), 2))
    .withColumn("sale_date_plus_9d", F.date_add("sale_date", 9))
)

result = (
    filtered
    .groupBy("store_id", "product_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
    )
    .filter(F.col("num_rows") >= 1)
    .orderBy(F.col("store_id").desc_nulls_last())
)
