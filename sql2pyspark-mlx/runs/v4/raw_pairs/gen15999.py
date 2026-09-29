from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.col("customer_id").isin([2, 26, 35]))
    .select(
        "id",
        "customer_id",
        "attributes",
        F.col("ship_to.city").alias("ship_city"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
    )
)

result = (
    unpacked
    .groupBy("ship_city")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("ship_city") == "New York").alias("new_york_events"),
    )
    .filter(F.col("new_york_events") > 1)
)
