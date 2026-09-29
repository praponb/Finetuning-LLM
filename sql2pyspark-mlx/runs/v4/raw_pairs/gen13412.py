from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.count(F.when(F.col("attr_key") == "device", 1)).alias("device_rows"),
        F.min("created_at").alias("started_at"),
    )
)
