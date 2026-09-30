from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.coalesce(F.col("attributes").getItem("device"), F.lit("unknown")).alias("device_or_default"),
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("direct")).alias("gift_or_default"),
        F.aggregate("items", F.lit(0.0), lambda acc, li: acc + li["qty"] * li["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("none")).alias("utm_source_or_default"),
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("cart_value") >= 1670, "LARGE").otherwise("SMALL").alias("cart_value_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.countDistinct("gift_or_default").alias("distinct_gift_or_defaults"),
        F.round(F.avg("cart_value"), 2).alias("avg_cart_value"),
    )
    .orderBy("duration_min")
)
