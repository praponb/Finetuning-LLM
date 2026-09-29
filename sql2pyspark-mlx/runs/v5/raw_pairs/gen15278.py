from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "attributes",
        F.array_contains(F.map_keys("attributes"), "utm_source").alias("has_utm_source"),
        F.coalesce(F.col("attributes")["device"], F.lit("unknown")).alias("device_or_default"),
    )
)

result = (
    flattened_attrs
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.count("*").alias("num_rows"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("num_rows").desc_nulls_last())
)
