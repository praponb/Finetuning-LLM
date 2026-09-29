from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "ship_to",
        "attributes",
        F.col("attributes")["gift"].alias("gift"),
        F.col("ship_to.country").alias("ship_country"),
    )
)

result = (
    flattened_attrs
    .groupBy("ship_country")
    .agg(
        F.count("*").alias("cnt"),
        F.size(F.collect_set("ship_country")).alias("num_ship_countries"),
        F.size(F.collect_set("ship_country")).alias("num_ship_countries_2"),
        F.sort_array(F.collect_set("ship_country")).alias("ship_country_list"),
        F.countDistinct("ship_country").alias("distinct_ship_countries"),
        F.count(F.when(F.col("ship_country") == "US", 1)).alias("us_count"),
    )
    .orderBy(F.col("us_count").desc())
)
