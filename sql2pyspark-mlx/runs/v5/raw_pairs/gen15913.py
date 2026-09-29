from pyspark.sql import functions as F

base = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-05-28").cast("date")) & (~F.col("carrier").eqNullSafe(F.lit("DHL"))))
    .withColumn("shipped_date_label", F.date_format("shipped_date", "yyyy-MM-dd"))
    .withColumn("clean_status", F.trim("status"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("N/A")))
)

result = (
    base
    .rollup(
        F.when(F.col("cost") >= 92, "LARGE").otherwise("SMALL").alias("cost_tier"),
        F.date_trunc("WEEK", "shipped_date").alias("week_start"),
    )
    .agg(
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("status") == "IN_TRANSIT", 1)), F.lit(0)), 2).alias("cost_per_in_transit"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
    )
    .orderBy(F.col("week_start").desc())
)
