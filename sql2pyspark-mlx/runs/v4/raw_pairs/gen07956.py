from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.coalesce(F.col("attributes")["gift"], F.lit("unknown")).alias("gift_or_default"),
        F.col("attributes")["device"].alias("device"),
        F.col("ship_to.country").alias("ship_country"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.count_if(F.col("ship_country") == "UK").alias("uk_events"),
        F.max("created_at").alias("last_seen"),
        F.count("*").alias("num_rows"),
    )
)
