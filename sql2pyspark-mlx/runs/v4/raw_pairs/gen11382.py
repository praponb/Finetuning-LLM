from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "ship_to",
        "attributes",
        F.coalesce(F.col("attributes").getItem("device"), F.lit("none")).alias("device_or_default"),
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("max_item_price") >= 190, "LARGE").otherwise("SMALL").alias("max_item_price_tier"),
    )
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.percentile_approx("max_item_price", 0.5).alias("max_item_price_p50"),
        F.percentile_approx("max_item_price", 0.9).alias("max_item_price_p90"),
        F.percentile_approx("max_item_price", 0.9).alias("max_item_price_p90_2"),
    )
    .orderBy(F.col("max_item_price_p90_2").asc_nulls_first())
)
