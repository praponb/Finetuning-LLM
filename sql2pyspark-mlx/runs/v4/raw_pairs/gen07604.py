from pyspark.sql import functions as F

flattened = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("code"))

enriched = (
    flattened
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.segment"), F.col("d.city"))
)

filtered = (
    enriched
    .filter(F.col("code").isNotNull())
    .select(
        "*",
        F.concat("city", F.lit(" - "), "code").alias("city_code"),
        F.lower("code").alias("code_lower"),
        F.coalesce("code", F.lit("N/A")).alias("resolved_code"),
    )
)

valid = (
    filtered
    .filter(F.col("city") == "New York")
    .select("*", F.trim("segment").alias("clean_segment"))
)

result = (
    valid
    .cube("code", "customer_id")
    .agg(
        F.count_if(F.col("code") == "SPRING10").alias("spring10_events"),
        F.count(F.when(F.col("segment") == "SMB", 1)).alias("smb_rows"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
    )
    .orderBy(F.col("spring10_events").desc())
)
