from pyspark.sql import functions as F

flattened = df_carts.select("id", F.explode_outer("coupon_codes").alias("element"))

base = (
    flattened
    .filter(F.col("element") == "BUNDLE")
    .withColumn("element_lower", F.lower("element"))
    .withColumn("element_prefix", F.substring("element", 1, 3))
    .withColumn("resolved_element", F.coalesce("element", F.lit("NONE")))
)

base_carts = (
    base
    .filter(F.col("element").isNotNull())
    .select(
        "id",
        "element",
        "element_lower",
        "element_prefix",
        "resolved_element",
        F.coalesce("element", F.lit("unassigned")).alias("element_filled"),
        F.initcap(F.trim("element")).alias("element_title"),
        F.coalesce("element", F.lit("UNKNOWN")).alias("element_clean"),
    )
)

filtered = (
    base_carts
    .filter(F.col("element").isin(["WELCOME5", "SPRING10", "BUNDLE"]))
    .select(
        "*",
        F.substring("element", 1, 3).alias("element_prefix_2"),
        F.substring("element", 1, 4).alias("element_prefix_3"),
    )
)

base_carts_2 = (
    filtered
    .filter(F.col("element").isNotNull())
    .withColumn("element_length", F.length("element"))
    .withColumn("element_clean_2", F.coalesce("element", F.lit("NONE")))
)

stats = (
    base_carts_2
    .groupBy(
        F.when(F.col("element_length") >= 87, "PREMIUM").otherwise("STANDARD").alias("element_length_tier"),
    )
    .agg(
        F.count_if(F.col("element") == "WELCOME5").alias("welcome5_events"),
        F.count(F.when(F.col("element") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.min("element_length").alias("min_element_length"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("element") == "WELCOME5", F.col("element_length"))).alias("welcome5_total_element_length"),
        F.sum("element_length").alias("total_element_length"),
    )
    .filter(F.col("row_count") < 1)
)

result = (
    stats
    .select(
        "element_length_tier",
        "welcome5_events",
        "welcome5_rows",
        "min_element_length",
        "total_element_length",
    )
    .orderBy(F.col("welcome5_events").desc_nulls_first(), "total_element_length")
)
