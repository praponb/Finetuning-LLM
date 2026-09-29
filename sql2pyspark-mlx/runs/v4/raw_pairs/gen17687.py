from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
        F.col("ship_to.country").alias("ship_country"),
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["qty"] * i["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
    )
)

result = (
    unpacked
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.percentile_approx("items_total", [0.25, 0.5, 0.75]).alias("items_total_quartiles"),
        F.countDistinct("ship_country").alias("distinct_ship_countries"),
        F.min("created_at").alias("started_at"),
    )
    .filter(F.col("distinct_ship_countries") >= 1)
    .orderBy(F.col("activity_day").desc())
)
