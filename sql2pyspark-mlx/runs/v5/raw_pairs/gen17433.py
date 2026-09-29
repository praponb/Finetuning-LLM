from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

result = (
    exploded
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"), "customer_id")
    .agg(
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.min("created_at").alias("started_at"),
        F.count_if(F.col("key") == "device").alias("device_events"),
    )
)
