from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

filtered = (
    lines
    .filter(F.col("created_at") > F.lit("2025-03-01 19:00:00").cast("timestamp"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "k",
        "v",
        F.array_contains("coupon_codes", "WELCOME5").alias("is_welcome5"),
        F.coalesce("k", F.lit("DIRECT")).alias("resolved_k"),
    )
)

enriched = (
    filtered
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.email"), F.col("b.segment"), F.col("b.name"))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select(
        "a.customer_id",
        "a.v",
        "a.is_welcome5",
        "a.resolved_k",
        "a.segment",
        F.col("b.email").alias("customer_email"),
        F.col("b.age"),
        F.col("b.credit_limit"),
    )
)

daily = (
    with_dims
    .groupBy(
        "segment",
        F.when(F.col("credit_limit") >= 15800, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("age", 0.5).alias("age_p50"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
    )
    .filter(F.col("age_p50") < 1000000)
)

result = (
    daily
    .select(
        "segment",
        "credit_limit_tier",
        "avg_age",
        "consumer_count",
        F.when(F.col("segment").rlike("^(Consumer|Corporate)$"), "CORE").when(F.col("segment").rlike("^(SMB)$"), "GROUP_A").otherwise("PRIORITY").alias("segment_zone"),
    )
    .orderBy(F.col("segment_zone").desc())
)
