from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

flattened = (
    lines
    .select(
        "id",
        "customer_id",
        "created_at",
        "attr_value",
        F.explode("coupon_codes").alias("code"),
    )
)

cleaned = (
    flattened
    .filter(F.col("created_at") > F.lit("2025-03-02 06:00:00").cast("timestamp"))
    .select(
        "*",
        F.hour("created_at").alias("created_at_hour"),
        F.when(F.col("code").isin(["BUNDLE", "VIP"]), "Group A").otherwise("Other").alias("code_group"),
    )
)

enriched = (
    cleaned
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select("src.*", F.col("c.city"), F.col("c.segment"))
)

summary = (
    enriched
    .groupBy(
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        "code",
        F.when(F.col("created_at_hour") >= 63, "LARGE").otherwise("SMALL").alias("created_at_hour_tier"),
    )
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.countDistinct("city").alias("distinct_cities"),
    )
)

result = (
    summary
    .select("activity_day", "code", "created_at_hour_tier", "corporate_count")
    .orderBy("code", F.col("created_at_hour_tier").desc())
)
