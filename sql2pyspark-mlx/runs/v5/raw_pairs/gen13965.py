from pyspark.sql import functions as F

lines = df_carts.select("id", F.explode_outer("coupon_codes").alias("value"))

valid = (
    lines
    .filter(F.col("value").isin(["WELCOME5", "VIP"]))
    .select(
        "*",
        F.coalesce("value", F.lit("unassigned")).alias("value_filled"),
        F.coalesce("value", F.lit("NONE")).alias("value_filled_2"),
    )
)

filtered = (
    valid
    .filter(F.col("value").isNotNull())
    .withColumn("value_lower", F.lower("value"))
    .withColumn("value_clean", F.coalesce("value", F.lit("N/A")))
)

filtered_carts = (
    filtered
    .filter(F.col("value") == "VIP")
    .withColumn("value_upper", F.upper("value"))
    .withColumn("value_filled_3", F.coalesce("value", F.lit("OTHER")))
)

cleaned = (
    filtered_carts
    .filter((F.col("value") == "BUNDLE") & (F.col("value").eqNullSafe(F.lit("WELCOME5"))))
    .select(
        "id",
        "value",
        "value_filled_2",
        "value_lower",
        "value_clean",
        "value_upper",
        "value_filled_3",
        F.initcap(F.trim("value")).alias("value_title"),
    )
)

summary = (
    cleaned
    .groupBy("value")
    .agg(
        F.size(F.collect_set("value")).alias("num_values"),
        F.countDistinct("value").alias("distinct_values"),
        F.count("*").alias("event_count"),
        F.count_if(F.col("value") == "SPRING10").alias("spring10_events"),
        F.countDistinct("value").alias("distinct_values_2"),
        F.sort_array(F.collect_set("value")).alias("value_list"),
    )
)

result = (
    summary
    .select(
        "value",
        "num_values",
        "distinct_values",
        "spring10_events",
        "distinct_values_2",
        "value_list",
        F.when(F.col("value").rlike("^(BUNDLE|WELCOME5)$"), "OTHER_ZONE").when(F.col("value").rlike("^(SPRING10|VIP)$"), "CORE").otherwise("GROUP_B").alias("value_zone"),
    )
)
