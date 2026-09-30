from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("unknown")).alias("utm_source_or_default"),
    )
)

result = (
    flattened_attrs
    .withColumn("hour_window", F.date_trunc("HOUR", "created_at"))
    .rollup("hour_window")
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min_2"),
        F.grouping("hour_window").alias("grouping_level"),
    )
)
