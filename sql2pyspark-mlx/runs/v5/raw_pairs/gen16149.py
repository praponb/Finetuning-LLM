from pyspark.sql import functions as F

parsed = (
    df_carts
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "ship_to",
        F.col("ship_to.country").alias("ship_country"),
        F.coalesce(F.col("attributes")["device"], F.lit("direct")).alias("device_or_default"),
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
        F.col("ship_to.city").alias("ship_city"),
        F.exists("items", lambda i: i["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("max_item_price") >= 217, "LARGE").otherwise("SMALL").alias("max_item_price_tier"),
    )
    .agg(
        F.count_if(F.col("ship_country") == "US").alias("us_events"),
        F.round(F.avg("max_item_price"), 2).alias("avg_max_item_price"),
        F.sort_array(F.collect_set("ship_city")).alias("ship_city_list"),
    )
    .filter(F.col("avg_max_item_price") < 1000000)
)
