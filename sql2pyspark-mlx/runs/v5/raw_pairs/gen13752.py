from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "attributes",
        F.col("ship_to.city").alias("ship_city"),
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
    )
)

result = (
    flattened_attrs
    .withColumn("max_item_price_tier", F.when(F.col("max_item_price") >= 270, "PREMIUM").otherwise("STANDARD"))
    .cube("max_item_price_tier", "customer_id", "ship_city")
    .agg(
        F.avg("weight_kg").alias("avg_weight_kg"),
        F.count(F.when(F.col("ship_city") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
        F.count_if(F.col("ship_city") == "Berlin").alias("berlin_events"),
        F.size(F.collect_set("ship_city")).alias("num_ship_cities"),
        F.grouping("max_item_price_tier").alias("grouping_level"),
    )
    .filter(F.col("chiang_mai_rows") >= 1)
    .orderBy(F.col("customer_id").desc())
)
