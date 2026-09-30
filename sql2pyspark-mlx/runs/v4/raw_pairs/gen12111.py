from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.size("items").alias("num_items"),
        F.transform("items", lambda x: x["product_id"]).alias("product_ids"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        F.date_trunc("DAY", "created_at").alias("activity_day"),
    )
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.min("created_at").alias("started_at"),
    )
    .orderBy(F.col("activity_day").desc())
)
