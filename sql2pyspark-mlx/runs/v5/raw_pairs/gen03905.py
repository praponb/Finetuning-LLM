from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

result = (
    exploded
    .groupBy("attr_key")
    .agg(
        F.count(F.when(F.col("attr_key") == "gift", 1)).alias("gift_rows"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("attr_key") == "weight_kg", 1)).alias("weight_kg_count"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
    )
    .orderBy(F.col("cnt").desc_nulls_last())
)
