from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        "attributes",
        F.coalesce(F.col("attributes")["gift"], F.lit("none")).alias("gift_or_default"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("unknown")).alias("utm_source_or_default"),
        F.transform(
            F.filter("items", lambda item: item["promo"].isNotNull()),
            lambda item: F.struct(item["product_id"].alias("pid"), F.round(item["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
    )
)

result = (
    parsed
    .withColumn("max_item_price_tier", F.when(F.col("max_item_price") >= 236, "PREMIUM").otherwise("STANDARD"))
    .cube("max_item_price_tier")
    .agg(
        F.countDistinct("utm_source_or_default").alias("distinct_utm_source_or_defaults"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count"),
        F.percentile_approx("max_item_price", [0.25, 0.5, 0.75]).alias("max_item_price_quartiles"),
        F.percentile_approx("max_item_price", 0.5).alias("max_item_price_p50"),
        F.grouping("max_item_price_tier").alias("grouping_id"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
