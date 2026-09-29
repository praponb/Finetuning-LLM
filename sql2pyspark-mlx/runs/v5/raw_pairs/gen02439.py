from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        "attributes",
        F.coalesce(F.col("attributes")["utm_source"], F.lit("none")).alias("utm_source_or_default"),
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
    )
)
