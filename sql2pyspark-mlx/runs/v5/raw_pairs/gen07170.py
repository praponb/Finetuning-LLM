from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.col("customer_id").isin([10, 26, 35, 39]))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        F.array_contains("coupon_codes", "VIP").alias("used_vip"),
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.col("attributes")["utm_source"].alias("utm_source"),
    )
)

result = (
    unpacked
    .withColumn("activity_day", F.date_trunc("day", "created_at"))
    .cube("activity_day", "customer_id")
    .agg(
        F.min("created_at").alias("first_created_at"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.max("created_at").alias("ended_at"),
        F.countDistinct("zip_or_unknown").alias("distinct_zip_or_unknowns"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes_2"),
        F.max("created_at").alias("ended_at_2"),
        F.grouping("activity_day").alias("gid"),
    )
    .orderBy(F.col("span_minutes_2").asc_nulls_last())
)
