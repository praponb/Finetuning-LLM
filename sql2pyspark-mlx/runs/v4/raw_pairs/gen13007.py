from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("key", "value"),
    )
)

matched_keys = (
    lines
    .select("id")
    .intersect(
        df_carts
        .filter(F.size("coupon_codes") >= 1)
        .select("id")
    )
)

verified = (
    lines
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    verified
    .filter(F.col("created_at") >= "2023-04-05")
    .withColumn("key_lower", F.lower("key"))
)

result = (
    valid
    .groupBy("customer_id")
    .agg(
        F.max("created_at").alias("ended_at"),
        F.count_if(F.col("key") == "weight_kg").alias("weight_kg_events"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
    )
    .orderBy(F.col("customer_id").desc_nulls_last())
)
