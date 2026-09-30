from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element")

flattened = df_carts.select("id", F.explode_outer("coupon_codes").alias("element"))

base = (
    flattened
    .filter(F.col("element").isNotNull() & (F.col("element").eqNullSafe(F.lit("VIP"))))
    .select(
        "*",
        F.trim("element").alias("clean_element"),
        F.trim("element").alias("clean_element_2"),
    )
)

cleaned = (
    base
    .filter(F.col("element").isNotNull())
    .withColumn("is_bundle", F.when(F.col("element") == "BUNDLE", 1).otherwise(0))
    .withColumn("element_filled", F.coalesce("element", F.lit("NONE")))
)

cleaned_carts = (
    cleaned
    .filter(F.col("element").isNotNull() & ~F.col("element").isin(["VIP", "WELCOME5", "SPRING10"]))
    .select(
        "*",
        F.round((F.col("is_bundle") * 0.9), 2).alias("adjusted_is_bundle"),
        F.coalesce("element", F.lit("OTHER")).alias("element_filled_2"),
    )
)

filtered = (
    cleaned_carts
    .filter(F.col("element").isNotNull() & (F.col("is_bundle") <= 54))
    .select(
        "element",
        "clean_element",
        "is_bundle",
        "adjusted_is_bundle",
        "element_filled_2",
        F.substring("element", 1, 3).alias("element_prefix"),
    )
)

cleaned_carts_2 = (
    filtered
    .filter(F.col("element").isin(["BUNDLE", "SPRING10"]))
    .withColumn("element_lower", F.lower("element"))
)

enriched = (
    cleaned_carts_2
    .select(
        "element",
        "element_prefix",
        F.count("is_bundle").over(w).alias("element_count_is_bundle"),
        F.sum("is_bundle").over(w).alias("element_sum_is_bundle"),
    )
)

summary = (
    enriched
    .groupBy("element")
    .agg(
        F.percentile_approx("element_count_is_bundle", [0.25, 0.5, 0.75]).alias("element_count_is_bundle_quartiles"),
        F.count_if(F.col("element") == "BUNDLE").alias("bundle_events"),
        F.sort_array(F.collect_set("element")).alias("element_list"),
        F.round(F.avg("element_count_is_bundle"), 2).alias("avg_element_count_is_bundle"),
        F.max(F.when(F.col("element") == "SPRING10", F.col("element_count_is_bundle"))).alias("spring10_max_element_count_is_bundle"),
    )
)

result = (
    summary
    .select(
        "element",
        "element_count_is_bundle_quartiles",
        "element_list",
        "spring10_max_element_count_is_bundle",
    )
    .orderBy(F.col("spring10_max_element_count_is_bundle").asc_nulls_last())
)
