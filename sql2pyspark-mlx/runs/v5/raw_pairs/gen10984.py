from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
        F.forall("items", lambda item: item["qty"] <= 5).alias("all_small_qty"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("unknown")).alias("utm_source_or_default"),
        F.col("attributes")["gift"].alias("gift"),
    )
)

result = (
    unpacked
    .groupBy("customer_id")
    .agg(
        F.percentile_approx("max_item_price", 0.5).alias("max_item_price_p50"),
        F.percentile_approx("max_item_price", 0.95).alias("max_item_price_p95"),
        F.round(F.avg("max_item_price"), 2).alias("avg_max_item_price"),
        F.percentile_approx("max_item_price", 0.9).alias("max_item_price_p90"),
        F.round(F.avg("max_item_price"), 2).alias("avg_max_item_price_2"),
    )
    .filter(F.col("avg_max_item_price_2") < 1000000)
    .orderBy(F.col("avg_max_item_price").desc())
)
