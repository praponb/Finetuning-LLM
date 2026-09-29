from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tag_length").desc_nulls_first())
w2 = Window.partitionBy("tag").orderBy(F.col("tag_length").desc_nulls_last())
w3 = Window.partitionBy("tag")
w4 = Window.partitionBy("tag").orderBy("tag_length_str")
w5 = Window.orderBy(F.col("tag_length").asc_nulls_first())

flattened = df_carts.select("id", F.explode_outer("coupon_codes").alias("tag"))

valid = (
    flattened
    .filter(F.col("tag").isNotNull() & (F.col("tag") == "SPRING10"))
    .withColumn("tag_length", F.length("tag"))
    .withColumn("tag_clean", F.coalesce("tag", F.lit("DIRECT")))
)

base = (
    valid
    .filter(F.col("tag") == "VIP")
    .withColumn("tag_length_str", F.col("tag_length").cast("double"))
    .withColumn("resolved_tag", F.coalesce("tag", F.lit("DIRECT")))
)

with_windows = (
    base
    .select(
        "*",
        F.rank().over(w).alias("tag_length_rank"),
        F.percent_rank().over(w2).alias("tag_length_percent_rank"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "tag",
        "tag_length",
        "tag_clean",
        "resolved_tag",
        "tag_length_rank",
        F.round((F.col("tag_length_rank") / F.sum("tag_length_rank").over(w3)), 4).alias("tag_length_rank_share"),
        F.cume_dist().over(w4).alias("tag_length_str_cume_dist"),
        F.percent_rank().over(w5).alias("tag_length_percent_rank_2"),
    )
)

result = (
    enriched
    .select(
        "tag",
        "tag_length",
        "tag_clean",
        "resolved_tag",
        "tag_length_rank_share",
        "tag_length_percent_rank_2",
    )
    .orderBy(F.col("tag_length").asc_nulls_last())
)
