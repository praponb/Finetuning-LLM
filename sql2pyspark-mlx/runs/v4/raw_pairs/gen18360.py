from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        "attributes",
        F.col("ship_to.country").alias("ship_country"),
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("none")).alias("gift_or_default"),
        F.col("ship_to.city").alias("ship_city"),
        F.exists("items", lambda li: li["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    parsed
    .rollup(
        "customer_id",
        F.date_trunc("DAY", "created_at").alias("activity_day"),
        "ship_country",
    )
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.count("*").alias("row_count"),
        F.size(F.collect_set("ship_city")).alias("num_ship_cities"),
    )
    .filter(F.col("row_count") < 1)
    .orderBy(F.col("customer_id").desc())
)
