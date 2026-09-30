from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-07-17").cast("date"))
    .select("*", F.add_months("sale_date", 1).alias("sale_date_plus_months"))
)

result = (
    valid
    .withColumn("quarter_start", F.date_trunc("QUARTER", "sale_date"))
    .withColumn("units_tier", F.when(F.col("units") >= 44, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["quarter_start", "units_tier"], ["quarter_start"]],
        "quarter_start",
        "units_tier",
    )
    .agg(
        F.bit_or("units").alias("units_bit_or"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.percentile_approx("units", 0.9).alias("units_p90"),
        F.count("*").alias("num_rows"),
        F.stddev_samp("units").alias("units_stddev"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
    )
    .filter(F.col("units_stddev") > 0)
    .orderBy(F.col("units_tier").desc_nulls_first())
)
