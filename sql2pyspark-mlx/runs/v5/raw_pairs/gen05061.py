from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "ship_to",
        F.array_contains(F.map_keys("attributes"), "utm_source").alias("has_utm_source"),
        F.coalesce(F.col("attributes")["gift"], F.lit("direct")).alias("gift_or_default"),
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.size("attributes").alias("num_attributes"),
        F.col("attributes")["gift"].alias("gift"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("day", "created_at").alias("activity_day"))
    .agg(
        F.round(F.avg("num_attributes"), 2).alias("avg_num_attributes"),
        F.countDistinct("city_or_unknown").alias("distinct_city_or_unknowns"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
    )
)
