from pyspark.sql import functions as F

result = (
    df_store_sales
    .withColumn("units_tier", F.when(F.col("units") >= 43, "PREMIUM").otherwise("STANDARD"))
    .withColumn("activity_month", F.date_trunc("MONTH", "sale_date"))
    .groupingSets(
        [["units_tier", "activity_month"], ["units_tier"]],
        "units_tier",
        "activity_month",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.min("units").alias("min_units"),
    )
    .orderBy(F.col("distinct_ids_2").asc_nulls_first())
)
