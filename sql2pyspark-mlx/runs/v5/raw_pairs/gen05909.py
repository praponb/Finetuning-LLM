from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "ship_to",
        "attributes",
        F.col("ship_to.zip").alias("ship_zip"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
    )
)

result = (
    parsed
    .groupBy(F.date_trunc("hour", "created_at").alias("hour_window"))
    .agg(
        F.count("*").alias("cnt"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.count("*").alias("cnt_2"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
