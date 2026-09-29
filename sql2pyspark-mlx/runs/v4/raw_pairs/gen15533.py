from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

result = (
    lines
    .groupBy("key")
    .agg(
        F.size(F.collect_set("key")).alias("num_keys"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count_if(F.col("key") == "utm_source").alias("utm_source_events"),
        F.sort_array(F.collect_set("key")).alias("key_list_2"),
        F.count(F.when(F.col("key") == "weight_kg", 1)).alias("weight_kg_rows"),
        F.count("*").alias("row_count"),
    )
)
