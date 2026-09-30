from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "value"),
    )
)

result = (
    flattened
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("row_count"),
        F.size(F.collect_set("k")).alias("num_ks"),
        F.count_if(F.col("k") == "weight_kg").alias("weight_kg_events"),
        F.countDistinct("k").alias("distinct_ks"),
        F.count(F.when(F.col("k") == "weight_kg", 1)).alias("weight_kg_rows"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
    )
    .orderBy("duration_min")
)
