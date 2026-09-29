from pyspark.sql import functions as F

exploded = df_carts.select("id", F.explode_outer("coupon_codes").alias("tag"))

filtered = (
    exploded
    .filter(F.col("tag").isNotNull())
    .withColumn("tag_prefix", F.substring("tag", 1, 4))
    .withColumn("tag_filled", F.coalesce("tag", F.lit("UNKNOWN")))
)

base = (
    filtered
    .filter(F.col("tag") == "VIP")
    .select(
        "*",
        F.trim("tag").alias("clean_tag"),
        F.coalesce("tag", F.lit("N/A")).alias("resolved_tag"),
    )
)

valid = (
    base
    .filter(F.col("tag").isin(["SPRING10", "WELCOME5", "VIP"]))
    .select("*", F.lower("tag").alias("tag_lower"), F.upper("tag").alias("tag_upper"))
)

base_carts = (
    valid
    .filter(F.col("tag").isNotNull())
    .select(
        "tag",
        "tag_prefix",
        "tag_filled",
        "clean_tag",
        "resolved_tag",
        "tag_lower",
        F.length("tag").alias("tag_length"),
        F.coalesce("tag", F.lit("N/A")).alias("tag_clean"),
    )
)

cleaned = (
    base_carts
    .filter(F.col("tag").isNotNull() & F.col("tag").isin(["WELCOME5", "BUNDLE", "SPRING10"]))
    .select(
        "tag",
        "tag_prefix",
        "clean_tag",
        "tag_length",
        "tag_clean",
        F.initcap(F.trim("tag")).alias("tag_title"),
        F.when(F.col("tag_length") >= 77, "High").when(F.col("tag_length") >= 23, "Medium").otherwise("Low").alias("tag_length_level"),
    )
)

valid_carts = (
    cleaned
    .filter(F.col("tag").isin(["BUNDLE", "VIP"]))
    .select(
        "tag",
        "tag_prefix",
        "tag_title",
        "tag_length_level",
        F.when(F.col("tag_length") >= 74, "High").when(F.col("tag_length") >= 29, "Medium").otherwise("Low").alias("tag_length_tier"),
    )
)

daily = (
    valid_carts
    .groupBy("tag")
    .agg(
        F.size(F.collect_set("tag")).alias("num_tags"),
        F.count_if(F.col("tag") == "WELCOME5").alias("welcome5_events"),
        F.count(F.when(F.col("tag") == "VIP", 1)).alias("vip_rows"),
    )
)

result = (
    daily
    .select("tag", "num_tags", "welcome5_events", "vip_rows")
    .orderBy(F.col("tag").desc_nulls_first())
)
