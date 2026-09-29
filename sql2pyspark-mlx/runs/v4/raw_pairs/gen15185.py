from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("k", "value"),
    )
)

result = (
    flattened
    .groupBy(
        "k",
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        F.date_trunc("DAY", "created_at").alias("activity_day"),
    )
    .agg(
        F.min("created_at").alias("started_at"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.min("created_at").alias("first_created_at"),
    )
)
