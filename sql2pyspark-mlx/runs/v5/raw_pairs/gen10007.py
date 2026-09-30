from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("attributes").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .groupBy("attr_key")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("attr_key") == "gift", 1)).alias("gift_rows"),
        F.count_if(F.col("attr_key") == "weight_kg").alias("weight_kg_events"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
    )
    .orderBy(F.col("num_attr_keys").desc())
)
