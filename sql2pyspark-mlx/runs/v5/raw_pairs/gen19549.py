from pyspark.sql import functions as F

flattened = (
    df_carts
    .withColumn("elem", F.explode_outer("items"))
    .select("id", "created_at", "coupon_codes", F.col("elem.qty"), F.col("elem.price"))
)

lines = flattened.select("created_at", "qty", F.explode_outer("coupon_codes").alias("code"))

filtered = (
    lines
    .filter(F.col("code").isNotNull())
    .withColumn("created_at_period", F.date_trunc("month", "created_at"))
)

base = (
    filtered
    .filter(F.col("code").isNotNull() & (F.col("code").eqNullSafe(F.lit("BUNDLE"))))
    .select(
        "*",
        F.month("created_at").alias("created_at_month"),
        F.round((F.col("qty") * 1.07), 2).alias("adjusted_qty"),
    )
)

result = (
    base
    .withColumn("week_start", F.date_trunc("week", "created_at_period"))
    .withColumn("created_at_month_tier", F.when(F.col("created_at_month") >= 72, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["week_start", "code", "created_at_month_tier"], ["week_start", "code"], ["week_start"]],
        "week_start",
        "code",
        "created_at_month_tier",
    )
    .agg(
        F.avg(F.when(F.col("code") == "BUNDLE", F.col("adjusted_qty"))).alias("bundle_avg_adjusted_qty"),
        F.percentile_approx("adjusted_qty", 0.95).alias("adjusted_qty_p95"),
        F.size(F.collect_set("code")).alias("num_codes"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("bundle_avg_adjusted_qty") > 0)
    .orderBy(F.col("code").desc())
)
