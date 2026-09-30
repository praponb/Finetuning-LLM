from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("none")).alias("gift_or_default"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("none")).alias("utm_source_or_default"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
    )
)

result = (
    unpacked
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .rollup("activity_day")
    .agg(
        F.count("*").alias("num_rows"),
        F.min("created_at").alias("first_seen"),
        F.count("*").alias("num_rows_2"),
        F.grouping("activity_day").alias("gid"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy(F.col("num_rows_2").desc())
)
