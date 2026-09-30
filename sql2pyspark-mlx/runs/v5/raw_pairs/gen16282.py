from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "items",
        "attributes",
        F.col("attributes").getItem("gift").alias("gift"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.size("items").alias("num_items"),
        F.coalesce(F.col("attributes")["gift"], F.lit("unknown")).alias("gift_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("num_items", 0.95).alias("num_items_p95"),
        F.percentile_approx("num_items", 0.95).alias("num_items_p95_2"),
        F.percentile_approx("num_items", 0.5).alias("num_items_p50"),
        F.percentile_approx("weight_kg", 0.5).alias("weight_kg_p50"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy(F.col("num_items_p50").desc_nulls_last())
)
