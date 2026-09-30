from pyspark.sql import functions as F

lines = df_carts.select("id", F.explode_outer("attributes").alias("attr_key", "attr_value"))

valid = (
    lines
    .filter(F.col("attr_key").isin(["gift", "device"]))
    .select("*", F.initcap(F.trim("attr_key")).alias("attr_key_title"))
)

base = (
    valid
    .filter(F.col("attr_key").isNotNull())
    .select("*", F.lower("attr_key").alias("attr_key_lower"))
)

filtered = (
    base
    .filter(~F.col("attr_key").isin(["gift", "weight_kg", "device"]))
    .withColumn("attr_key_upper", F.upper("attr_key"))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("NONE")))
)

filtered_carts = (
    filtered
    .filter(F.col("attr_key") == "gift")
    .withColumn("clean_attr_key", F.trim("attr_key"))
    .withColumn("attr_key_clean_2", F.coalesce("attr_key", F.lit("NONE")))
)

cleaned = (
    filtered_carts
    .filter(F.col("attr_key") == "gift")
    .select(
        "*",
        F.initcap(F.trim("attr_key")).alias("attr_key_title_2"),
        F.coalesce("attr_key", F.lit("N/A")).alias("attr_key_filled"),
    )
)

result = (
    cleaned
    .select(
        "attr_key",
        "attr_key_lower",
        "attr_key_clean",
        "clean_attr_key",
        "attr_key_title_2",
        F.when(F.col("attr_key").rlike("^(device|weight_kg)$"), "CORE").when(F.col("attr_key").rlike("^(utm_source|gift)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("attr_key_zone"),
    )
)
