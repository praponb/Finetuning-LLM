from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.col("created_at") < "2023-04-19")
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        "attributes",
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("none")).alias("gift_or_default"),
        F.col("attributes")["gift"].alias("gift"),
    )
)

result = (
    unpacked
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("event_count"),
        F.max("created_at").alias("last_seen"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("cnt"),
    )
    .orderBy("customer_id")
)
