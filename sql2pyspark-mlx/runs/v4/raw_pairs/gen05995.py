from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.array_contains("coupon_codes", "BUNDLE"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        F.transform(
            F.filter("items", lambda item: item["promo"].isNotNull()),
            lambda item: F.struct(item["product_id"].alias("pid"), F.round(item["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.col("ship_to.country").alias("ship_country"),
        F.array_contains(F.map_keys("attributes"), "device").alias("has_device"),
    )
)

result = (
    unpacked
    .groupBy("ship_country")
    .agg(
        F.max("created_at").alias("ended_at"),
        F.sort_array(F.collect_set("ship_country")).alias("ship_country_list"),
        F.count(F.when(F.col("ship_country") == "SG", 1)).alias("sg_count"),
        F.sort_array(F.collect_set("ship_country")).alias("ship_country_list_2"),
    )
    .filter(F.col("sg_count") > 1)
)
