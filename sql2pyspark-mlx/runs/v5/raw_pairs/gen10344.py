from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.col("customer_id").isin([22, 24]))
    .select(
        "id",
        "customer_id",
        "items",
        "ship_to",
        "attributes",
        F.coalesce(F.col("attributes")["device"], F.lit("unknown")).alias("device_or_default"),
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.forall("items", lambda li: li["qty"] <= 5).alias("all_small_qty"),
        F.array_contains(F.map_keys("attributes"), "device").alias("has_device"),
        F.exists("items", lambda li: li["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    unpacked
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.countDistinct("customer_id").alias("distinct_customer_ids_2"),
        F.countDistinct("device_or_default").alias("distinct_device_or_defaults"),
    )
    .orderBy("distinct_customer_ids_2")
)
