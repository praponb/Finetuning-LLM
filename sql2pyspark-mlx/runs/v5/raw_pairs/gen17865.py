from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.array_contains("coupon_codes", "WELCOME5"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        "attributes",
        F.array_contains(F.map_keys("attributes"), "utm_source").alias("has_utm_source"),
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.size("items").alias("num_items"),
        F.col("ship_to.country").alias("ship_country"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("none")).alias("utm_source_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("num_items") >= 2, "LARGE").otherwise("SMALL").alias("num_items_tier"),
        "ship_country",
    )
    .agg(
        F.percentile_approx("num_items", 0.5).alias("num_items_p50"),
        F.max("created_at").alias("last_created_at"),
        F.min("created_at").alias("first_created_at"),
    )
    .filter(F.col("num_items_p50") > 0)
)
