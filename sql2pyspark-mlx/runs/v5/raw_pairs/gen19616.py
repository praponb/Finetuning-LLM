from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
        F.size(F.filter("items", lambda i: i["qty"] >= 3)).alias("bulk_lines"),
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.array_contains("coupon_codes", "SPRING10").alias("used_spring10"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("bulk_lines") >= 3, "LARGE").otherwise("SMALL").alias("bulk_lines_tier"),
        "customer_id",
    )
    .agg(
        F.percentile_approx("bulk_lines", 0.9).alias("bulk_lines_p90"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.percentile_approx("bulk_lines", 0.5).alias("bulk_lines_p50"),
        F.percentile_approx("max_item_price", 0.95).alias("max_item_price_p95"),
        F.percentile_approx("max_item_price", 0.9).alias("max_item_price_p90"),
        F.min("max_item_price").alias("min_max_item_price"),
    )
    .filter(F.col("bulk_lines_p50") >= 0)
    .orderBy("bulk_lines_p50")
)
