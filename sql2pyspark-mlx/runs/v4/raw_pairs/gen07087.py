from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("revenue") < 3800)
    .withColumn("sale_date_dayofweek", F.dayofweek("sale_date"))
)

result = (
    filtered
    .groupBy(F.when(F.col("units") >= 38, "HIGH").otherwise("LOW").alias("units_tier"))
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.count("*").alias("row_count"),
        F.percentile_approx("units", 0.5).alias("units_p50"),
    )
)
