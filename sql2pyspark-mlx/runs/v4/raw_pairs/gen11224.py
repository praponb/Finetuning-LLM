from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("delivered_date") > "2025-08-21"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        F.upper("carrier").alias("carrier_upper"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled"),
    )
)

result = (
    filtered
    .withColumn("cost_tier", F.when(F.col("cost") >= 88, "LARGE").otherwise("SMALL"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "shipped_date"))
    .rollup("cost_tier", "quarter_start")
    .agg(
        F.max(F.when(F.col("carrier") == "Kerry", F.col("cost"))).alias("kerry_max_cost"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("quarter_start").desc_nulls_first())
)
