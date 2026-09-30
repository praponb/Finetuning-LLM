from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "v"),
    )
)

result = (
    exploded
    .withColumn("hour_window", F.date_trunc("hour", "created_at"))
    .groupingSets(
        [["hour_window", "customer_id"], ["hour_window"]],
        "hour_window",
        "customer_id",
    )
    .agg(
        F.max("created_at").alias("last_created_at"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.max("created_at").alias("last_seen"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.count("*").alias("num_rows"),
    )
    .orderBy("hour_window")
)
