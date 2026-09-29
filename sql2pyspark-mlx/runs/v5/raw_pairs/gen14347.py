from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.forall("items", lambda li: li["qty"] <= 5).alias("all_small_qty"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.array_contains(F.map_keys("attributes"), "device").alias("has_device"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("weight_kg") >= 3.7, "PREMIUM").otherwise("STANDARD").alias("weight_kg_tier"),
    )
    .agg(
        F.round(F.avg("weight_kg"), 2).alias("avg_weight_kg"),
        F.percentile_approx("weight_kg", [0.25, 0.5, 0.75]).alias("weight_kg_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("weight_kg", 0.95).alias("weight_kg_p95"),
    )
    .orderBy(F.col("weight_kg_tier").asc_nulls_last())
)
