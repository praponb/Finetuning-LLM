from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "ship_to",
        F.size("attributes").alias("num_attributes"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("unknown")).alias("utm_source_or_default"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)

result = (
    flattened_attrs
    .rollup(
        F.when(F.col("num_attributes") >= 70, "PREMIUM").otherwise("STANDARD").alias("num_attributes_tier"),
    )
    .agg(
        F.round(F.avg("num_attributes"), 2).alias("avg_num_attributes"),
        F.min("num_attributes").alias("min_num_attributes"),
        F.countDistinct("utm_source_or_default").alias("distinct_utm_source_or_defaults"),
    )
    .filter(F.col("min_num_attributes") > 0)
)
