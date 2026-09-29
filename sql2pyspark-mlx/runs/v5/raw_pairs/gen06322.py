from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        "ship_to",
        F.coalesce(F.col("attributes").getItem("device"), F.lit("direct")).alias("device_or_default"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    parsed
    .groupBy("ship_city")
    .agg(
        F.count("*").alias("num_rows"),
        F.sort_array(F.collect_set("ship_city")).alias("ship_city_list"),
        F.sort_array(F.collect_set("ship_city")).alias("ship_city_list_2"),
        F.count(F.when(F.col("ship_city") == "Berlin", 1)).alias("berlin_count"),
        F.sort_array(F.collect_set("ship_city")).alias("ship_city_list_3"),
        F.count(F.when(F.col("ship_city") == "Berlin", 1)).alias("berlin_rows"),
    )
)
