from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "coupon_codes", F.explode("attributes").alias("attr_key", "v"))
)

result = (
    flattened
    .cube("attr_key")
    .agg(
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.count(F.when(F.col("attr_key") == "weight_kg", 1)).alias("weight_kg_rows"),
        F.count_if(F.col("attr_key") == "gift").alias("gift_events"),
        F.grouping("attr_key").alias("gid"),
    )
    .orderBy("attr_key")
)
