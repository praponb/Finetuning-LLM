from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-03-28").cast("date"))
    .withColumn("sale_date_plus_82d", F.date_add("sale_date", 82))
)

result = (
    base
    .groupBy(
        "store_id",
        F.when(F.col("revenue") >= 3300, "LARGE").otherwise("SMALL").alias("revenue_tier"),
        "product_id",
    )
    .agg(
        F.percentile_approx("units", 0.9).alias("units_p90"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
        F.bit_and("units").alias("units_bit_and"),
    )
    .orderBy(F.col("revenue_tier").desc_nulls_last())
)
