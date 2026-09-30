from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("tag"))
)

result = (
    exploded
    .withColumn("activity_day", F.date_trunc("day", "created_at"))
    .withColumn("hour_window", F.date_trunc("hour", "created_at"))
    .groupingSets(
        [["activity_day", "hour_window"], ["activity_day"], []],
        "activity_day",
        "hour_window",
    )
    .agg(
        F.count_if(F.col("tag") == "WELCOME5").alias("welcome5_events"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.count_if(F.col("tag") == "WELCOME5").alias("welcome5_events_2"),
    )
    .filter(F.col("duration_min") > 0)
    .orderBy(F.col("hour_window").desc_nulls_first())
)
