from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("event_count").desc())

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "attr_value"),
    )
)

exploded = lines.select("k", F.explode("coupon_codes").alias("value"))

filtered = (
    exploded
    .filter(F.col("value").isNotNull() & F.col("k").isNotNull())
    .select("*", F.substring("value", 1, 2).alias("value_prefix"))
)

cleaned = (
    filtered
    .filter(F.col("value").isin(["BUNDLE", "VIP", "SPRING10"]))
    .select(
        "*",
        F.coalesce("value", F.lit("Unknown")).alias("value_filled"),
        F.length("value").alias("value_length"),
    )
)

result = (
    cleaned
    .groupBy("k", "value")
    .agg(
        F.sort_array(F.collect_set("value")).alias("value_list"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("value") == "VIP", 1)).alias("vip_count"),
    )
    .withColumn("event_count_rank", F.rank().over(w))
)
