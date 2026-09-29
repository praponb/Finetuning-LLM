from pyspark.sql import functions as F

exploded = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("value"))

base = (
    exploded
    .filter(F.col("value").isNotNull() & (F.col("created_at") > F.lit("2025-03-01").cast("date")))
    .select(
        "*",
        F.upper("value").alias("value_upper"),
        F.initcap(F.trim("value")).alias("value_title"),
    )
)

filtered = (
    base
    .filter(F.col("created_at") >= F.lit("2025-03-01 23:00:00").cast("timestamp"))
    .withColumn("created_at_label", F.date_format("created_at", "yyyy-MM"))
)

base_carts = (
    filtered
    .filter((F.col("created_at") >= F.lit("2025-03-02").cast("date")) & F.col("value").isNotNull())
    .select(
        "id",
        "value",
        "value_upper",
        "value_title",
        "created_at_label",
        F.when(F.col("value") == "BUNDLE", 1).otherwise(0).alias("is_bundle"),
        F.coalesce("value", F.lit("OTHER")).alias("resolved_value"),
    )
)

valid = (
    base_carts
    .filter(F.col("is_bundle").between(42, 58))
    .select(
        "id",
        "value",
        "value_upper",
        "value_title",
        "created_at_label",
        "resolved_value",
        F.round("is_bundle", 2).alias("is_bundle_rounded"),
        F.coalesce("value", F.lit("DIRECT")).alias("resolved_value_2"),
    )
)

result = (
    valid
    .select(
        "id",
        "value",
        "value_title",
        "created_at_label",
        "is_bundle_rounded",
        "resolved_value_2",
    )
    .orderBy(F.col("id").desc())
)
