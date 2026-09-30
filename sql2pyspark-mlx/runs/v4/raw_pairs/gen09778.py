from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        "ship_to",
        "attributes",
        F.coalesce(F.col("attributes")["device"], F.lit("direct")).alias("device_or_default"),
        F.transform(
            F.filter("items", lambda i: i["promo"].isNotNull()),
            lambda i: F.struct(i["product_id"].alias("pid"), F.round(i["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
    )
)

result = (
    unpacked
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.count("*").alias("event_count"),
        F.min("created_at").alias("first_created_at"),
        F.max("created_at").alias("ended_at"),
        F.min("created_at").alias("first_seen"),
    )
    .filter(F.col("event_count") < 1)
)
