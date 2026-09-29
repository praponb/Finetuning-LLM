from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "created_at", F.explode_outer("coupon_codes").alias("element"))
)

result = (
    lines
    .groupBy(
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
    )
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.count("*").alias("event_count"),
        F.count_if(F.col("element") == "VIP").alias("vip_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .orderBy(F.col("distinct_ids_2").asc_nulls_first())
)
