from pyspark.sql import functions as F

lines = df_carts.select("id", "coupon_codes", F.explode_outer("attributes").alias("k", "v"))

base = (
    lines
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .select(
        "*",
        F.initcap(F.trim("k")).alias("k_title"),
        F.coalesce("k", F.lit("Unknown")).alias("k_filled"),
    )
)

base_carts = (
    base
    .filter(F.array_contains("coupon_codes", "BUNDLE"))
    .select(
        "*",
        F.array_join("coupon_codes", ", ").alias("coupon_codes_text"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_clean"),
    )
)

result = (
    base_carts
    .cube("k")
    .agg(
        F.count_if(F.col("k") == "weight_kg").alias("weight_kg_events"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.count(F.when(F.col("k") == "utm_source", 1)).alias("utm_source_rows"),
        F.count_if(F.col("k") == "weight_kg").alias("weight_kg_events_2"),
        F.countDistinct("k").alias("distinct_ks"),
        F.grouping("k").alias("gid"),
    )
)
