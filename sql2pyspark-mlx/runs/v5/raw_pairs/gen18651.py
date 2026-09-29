from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.size("coupon_codes") > 2)
    .select(
        "id",
        "coupon_codes",
        "attributes",
        F.exists("items", lambda li: li["price"] > 100).alias("has_premium_item"),
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
        F.col("attributes")["device"].alias("device"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("max_item_price") >= 235, "PREMIUM").otherwise("STANDARD").alias("max_item_price_tier"),
    )
    .agg(
        F.max("max_item_price").alias("max_max_item_price"),
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy("event_count_2")
)
