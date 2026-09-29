from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "attr_value"),
    )
)

result = (
    lines
    .cube("k", "customer_id")
    .agg(
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.count(F.when(F.col("k") == "utm_source", 1)).alias("utm_source_count"),
        F.grouping_id().alias("grouping_level"),
    )
)
