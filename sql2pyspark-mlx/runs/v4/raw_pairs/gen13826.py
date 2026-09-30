from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .groupBy("k")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("k")).alias("num_ks"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("k") == "device", 1)).alias("device_count"),
        F.count("*").alias("num_rows_2"),
        F.countDistinct("k").alias("distinct_ks"),
    )
)
