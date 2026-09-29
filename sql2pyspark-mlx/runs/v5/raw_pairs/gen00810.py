from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "attr_value"),
    )
)

lines = (
    exploded
    .select(
        "id",
        "created_at",
        "key",
        "attr_value",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

filtered = (
    lines
    .filter((F.col("created_at") >= F.lit("2025-03-02").cast("date")) & (F.col("value") == "WELCOME5"))
    .select(
        "*",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
        F.month("created_at").alias("created_at_month"),
    )
)

kept_keys = (
    filtered
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.hour("created_at").between(10, 17))
        .select("id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter((F.col("created_at") >= F.lit("2025-03-01 17:00:00").cast("timestamp")) & F.col("key").isNotNull())
    .select(
        "*",
        F.coalesce("days_since_created_at", F.lit(0)).alias("days_since_created_at_filled"),
        (F.col("days_since_created_at") + F.col("created_at_month")).alias("days_since_created_at_plus_created_at_month"),
        F.coalesce("key", F.lit("NONE")).alias("resolved_key"),
    )
)

stats = (
    base
    .withColumn("days_since_created_at_tier", F.when(F.col("days_since_created_at") >= 62, "HIGH").otherwise("LOW"))
    .withColumn("days_since_created_at_plus_created_at_month_tier", F.when(F.col("days_since_created_at_plus_created_at_month") >= 77, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["days_since_created_at_tier", "days_since_created_at_plus_created_at_month_tier"], ["days_since_created_at_tier"]],
        "days_since_created_at_tier",
        "days_since_created_at_plus_created_at_month_tier",
    )
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.percentile_approx("created_at_month", 0.5).alias("created_at_month_p50"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    stats
    .select(
        "days_since_created_at_tier",
        "days_since_created_at_plus_created_at_month_tier",
        "duration_min",
        "created_at_month_p50",
        "gid",
    )
    .orderBy(F.col("days_since_created_at_plus_created_at_month_tier").desc())
)
