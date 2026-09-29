from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "ship_to",
        F.col("ship_to.zip").alias("ship_zip"),
        F.col("ship_to.country").alias("ship_country"),
        F.coalesce(F.col("attributes")["device"], F.lit("none")).alias("device_or_default"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.transform("items", lambda item: item["product_id"]).alias("product_ids"),
    )
)

result = (
    flattened_attrs
    .groupBy("ship_country")
    .agg(
        F.count_if(F.col("ship_country") == "DE").alias("de_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("ship_country") == "SG").alias("sg_events"),
        F.count(F.when(F.col("ship_country") == "TH", 1)).alias("th_rows"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy(F.col("th_rows").desc())
)
