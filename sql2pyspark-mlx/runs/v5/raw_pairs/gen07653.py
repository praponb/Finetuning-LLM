from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.coalesce(F.col("attributes")["device"], F.lit("none")).alias("device_or_default"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    parsed
    .groupBy("ship_city")
    .agg(
        F.count(F.when(F.col("ship_city") == "Tokyo", 1)).alias("tokyo_count"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("ship_city") == "London", 1)).alias("london_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.sort_array(F.collect_set("ship_city")).alias("ship_city_list"),
    )
)
