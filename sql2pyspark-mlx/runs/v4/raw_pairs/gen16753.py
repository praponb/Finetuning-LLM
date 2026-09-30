from pyspark.sql import functions as F

exploded = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("code"))

filtered = (
    exploded
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("code").isNotNull() & F.col("code").isin(["BUNDLE", "SPRING10", "VIP"]))
    .select(
        "*",
        F.col("created_at").cast("date").alias("created_at_day"),
        F.coalesce("code", F.lit("OTHER")).alias("code_clean"),
    )
)

cleaned = (
    filtered
    .filter(F.month("created_at_day").isin([1, 10, 12]))
    .select(
        "id",
        "created_at",
        "code",
        "created_at_day",
        "code_clean",
        F.trim("code").alias("clean_code"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("day", "created_at").alias("activity_day"))
    .agg(
        F.min("created_at").alias("started_at"),
        F.max("created_at").alias("last_created_at"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") >= 1)
    .orderBy(F.col("event_count").desc())
)
