from pyspark.sql import functions as F

exploded = df_carts.select("id", "created_at", F.explode("attributes").alias("key", "value"))

matched_keys = (
    exploded
    .select("id")
    .intersect(
        df_carts
        .filter(F.col("customer_id").isin([15, 21, 25]))
        .select("id")
    )
)

kept_rows = (
    exploded
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("created_at") >= F.lit("2025-03-02").cast("date"))
    .withColumn("created_at_date", F.to_date("created_at"))
)

base_carts = (
    base
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.when(F.col("key").isin(["gift", "weight_kg"]), "Tier 1").otherwise("Other").alias("key_group"),
    )
)

result = (
    base_carts
    .groupBy("key")
    .agg(
        F.size(F.collect_set("key")).alias("num_keys"),
        F.count(F.when(F.col("key") == "gift", 1)).alias("gift_rows"),
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.size(F.collect_set("key")).alias("num_keys_2"),
    )
    .orderBy(F.col("key").desc_nulls_last())
)
