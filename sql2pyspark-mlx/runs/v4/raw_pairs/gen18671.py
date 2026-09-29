from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
        F.coalesce(F.col("attributes")["device"], F.lit("none")).alias("device_or_default"),
    )
)

result = (
    parsed
    .groupBy("customer_id")
    .agg(
        F.percentile_approx("max_item_price", 0.5).alias("max_item_price_p50"),
        F.percentile_approx("max_item_price", 0.9).alias("max_item_price_p90"),
        F.sum("max_item_price").alias("total_max_item_price"),
        F.max("max_item_price").alias("max_max_item_price"),
    )
    .orderBy(F.col("max_max_item_price").desc_nulls_last())
)
