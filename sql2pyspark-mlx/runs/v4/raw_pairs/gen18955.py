from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "created_at",
        "attributes",
        F.array_contains(F.map_keys("attributes"), "utm_source").alias("has_utm_source"),
        F.forall("items", lambda li: li["qty"] <= 5).alias("all_small_qty"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.col("ship_to.country").alias("ship_country"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("none")).alias("utm_source_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.max("created_at").alias("last_created_at"),
        F.max("created_at").alias("last_seen"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("span_minutes") > 0)
)
