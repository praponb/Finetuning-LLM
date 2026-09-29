from pyspark.sql import functions as F

parsed = (
    df_carts
    .filter(F.col("created_at") < "2023-05-01")
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("ship_to.zip").alias("ship_zip"),
        F.col("ship_to.country").alias("ship_country"),
        F.size("items").alias("num_items"),
        F.array_max(F.transform("items", lambda x: x["price"])).alias("max_item_price"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("unknown")).alias("utm_source_or_default"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("num_items") >= 3, "LARGE").otherwise("SMALL").alias("num_items_tier"),
        "ship_country",
    )
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.sort_array(F.collect_set("ship_country")).alias("ship_country_list"),
        F.round(F.sum("max_item_price") / F.nullif(F.count(F.when(F.col("ship_country") == "UK", 1)), F.lit(0)), 2).alias("max_item_price_per_uk"),
    )
)
