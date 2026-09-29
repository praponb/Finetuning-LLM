from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

lines = flattened.select("id", "v", F.explode("coupon_codes").alias("value"))

filtered = (
    lines
    .filter(F.col("value").isin(["BUNDLE", "WELCOME5", "VIP"]))
    .select(
        "*",
        F.when(F.col("value").isin(["SPRING10", "WELCOME5"]), "Core").otherwise("Other").alias("value_group"),
        F.lower("value").alias("value_lower"),
    )
)

cleaned = (
    filtered
    .filter(F.col("value").isNotNull())
    .select(
        "id",
        "v",
        "value",
        "value_group",
        "value_lower",
        F.trim("value").alias("clean_value"),
        F.coalesce("value", F.lit("OTHER")).alias("resolved_value"),
    )
)

filtered_carts = (
    cleaned
    .filter(F.col("value") == "SPRING10")
    .withColumn("value_title", F.initcap(F.trim("value")))
)

result = (
    filtered_carts
    .groupBy("value")
    .agg(
        F.size(F.collect_set("value")).alias("num_values"),
        F.count(F.when(F.col("value") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.count(F.when(F.col("value") == "BUNDLE", 1)).alias("bundle_count"),
        F.count_if(F.col("value") == "BUNDLE").alias("bundle_events"),
        F.count(F.when(F.col("value") == "BUNDLE", 1)).alias("bundle_count_2"),
    )
    .orderBy("value")
)
