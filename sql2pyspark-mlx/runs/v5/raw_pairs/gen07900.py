from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("value"))
)

result = (
    exploded
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"), "customer_id")
    .agg(
        F.max("created_at").alias("last_seen"),
        F.count_if(F.col("value") == "WELCOME5").alias("welcome5_events"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.max("created_at").alias("ended_at"),
    )
    .filter(F.col("span_minutes") < 1000000)
)
