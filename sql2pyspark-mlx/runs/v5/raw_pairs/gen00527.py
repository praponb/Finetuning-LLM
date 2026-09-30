from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

result = (
    lines
    .groupBy("customer_id")
    .agg(
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.count_if(F.col("attr_key") == "utm_source").alias("utm_source_events"),
    )
    .filter(F.col("num_attr_keys") >= 1)
    .orderBy("utm_source_events")
)
