from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.array_contains(F.map_keys("attributes"), "device").alias("has_device"),
        F.col("attributes").getItem("device").alias("device"),
        F.transform("items", lambda i: i["product_id"]).alias("product_ids"),
        F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["qty"] * i["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("items_total") >= 1630, "LARGE").otherwise("SMALL").alias("items_total_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("items_total", 0.5).alias("items_total_p50"),
        F.percentile_approx("items_total", 0.9).alias("items_total_p90"),
    )
    .orderBy("items_total_p50")
)
