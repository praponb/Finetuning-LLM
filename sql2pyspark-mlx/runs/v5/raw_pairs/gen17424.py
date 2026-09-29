from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        "ship_to",
        F.col("attributes")["device"].alias("device"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("unknown")).alias("gift_or_default"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.array_contains(F.map_keys("attributes"), "device").alias("has_device"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.min("created_at").alias("first_created_at"),
        F.round(F.avg("weight_kg"), 2).alias("avg_weight_kg"),
        F.round(F.avg("weight_kg"), 2).alias("avg_weight_kg_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("weight_kg", [0.25, 0.5, 0.75]).alias("weight_kg_quartiles"),
    )
    .filter(F.col("distinct_ids") < 1000)
    .orderBy(F.col("avg_weight_kg").asc_nulls_last())
)
