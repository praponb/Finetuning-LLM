from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.array_contains("coupon_codes", "BUNDLE"))
    .select(
        "id",
        "coupon_codes",
        "items",
        F.size("items").alias("num_items"),
        F.col("attributes")["device"].alias("device"),
    )
)

result = (
    unpacked
    .withColumn("num_items_tier", F.when(F.col("num_items") >= 3, "HIGH").otherwise("LOW"))
    .rollup("num_items_tier")
    .agg(
        F.percentile_approx("num_items", 0.9).alias("num_items_p90"),
        F.count("*").alias("row_count"),
        F.max("num_items").alias("max_num_items"),
        F.percentile_approx("num_items", [0.25, 0.5, 0.75]).alias("num_items_quartiles"),
        F.grouping("num_items_tier").alias("gid"),
    )
)
