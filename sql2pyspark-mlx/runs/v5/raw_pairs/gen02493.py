from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("tag_length")
w2 = Window.partitionBy("tag")

exploded = df_carts.select("id", F.explode("coupon_codes").alias("tag"))

valid = (
    exploded
    .filter(F.col("tag").isNotNull())
    .select("*", F.trim("tag").alias("clean_tag"))
)

base = (
    valid
    .filter(F.col("tag").isin(["VIP", "WELCOME5", "BUNDLE"]))
    .select("*", F.length("tag").alias("tag_length"))
)

metrics = (
    base
    .filter(F.col("tag_length").isNotNull())
    .select(
        "id",
        "tag",
        "clean_tag",
        "tag_length",
        F.dense_rank().over(w).alias("tag_length_dense_rank"),
    )
)

base_carts = (
    metrics
    .filter(F.col("tag_length_dense_rank") < 3)
    .select(
        "id",
        "tag",
        "clean_tag",
        "tag_length_dense_rank",
        F.upper("tag").alias("tag_upper"),
        F.coalesce("tag", F.lit("UNKNOWN")).alias("tag_filled"),
    )
)

enriched = (
    base_carts
    .withColumn("tag_sum_tag_length_dense_rank", F.sum("tag_length_dense_rank").over(w2))
)

daily = (
    enriched
    .groupBy("tag")
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    daily
    .select("tag", "distinct_ids")
    .orderBy("tag", "distinct_ids")
)
