from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tag")
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("tag").orderBy(F.col("created_at_year_num").desc_nulls_last())
w4 = Window.orderBy(F.col("created_at_year_per_tag_count_created_at_year").desc_nulls_last())

flattened = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("tag"))

base = (
    flattened
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & (F.to_date("created_at") >= "2023-12-01"))
    .withColumn("created_at_year", F.year("created_at"))
)

scored = (
    base
    .filter(F.col("tag").isNotNull())
    .withColumn("tag_count_created_at_year", F.count("created_at_year").over(w))
)

cleaned = (
    scored
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & (F.col("created_at") >= "2024-03-14"))
    .select(
        "*",
        F.round((F.col("tag_count_created_at_year") * 1.15), 2).alias("adjusted_tag_count_created_at_year"),
        F.coalesce("tag", F.lit("N/A")).alias("tag_filled"),
    )
)

cleaned_carts = (
    cleaned
    .filter(F.col("created_at_year").between(54, 67))
    .withColumn("created_at_year_num", F.col("created_at_year").cast("double"))
    .withColumn("created_at_year_per_tag_count_created_at_year", F.round((F.col("created_at_year") / F.col("tag_count_created_at_year")), 2))
)

metrics = (
    cleaned_carts
    .select(
        "id",
        "tag",
        "tag_count_created_at_year",
        "adjusted_tag_count_created_at_year",
        "tag_filled",
        "created_at_year_per_tag_count_created_at_year",
        F.sum("created_at_year").over(w2).alias("created_at_year_sum_1h"),
        F.min("created_at_year_num").over(w).alias("tag_min_created_at_year_num"),
        F.cume_dist().over(w3).alias("created_at_year_num_cume_dist"),
        F.rank().over(w4).alias("created_at_year_per_tag_count_created_at_year_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("tag").isNotNull() & (F.col("created_at_year_sum_1h") <= 297))
    .select(
        "tag",
        "tag_count_created_at_year",
        "adjusted_tag_count_created_at_year",
        "created_at_year_per_tag_count_created_at_year",
        "created_at_year_sum_1h",
        F.col("created_at_year_per_tag_count_created_at_year_rank").cast("double").alias("created_at_year_per_tag_count_created_at_year_rank_num"),
        (F.col("created_at_year_num_cume_dist") / F.nullif("created_at_year_per_tag_count_created_at_year", F.lit(0))).alias("created_at_year_num_cume_dist_per_created_at_year_per_tag_count_created_at_year"),
        F.coalesce("tag", F.lit("DIRECT")).alias("tag_filled_2"),
    )
)

result = (
    valid
    .select(
        "tag",
        "tag_count_created_at_year",
        "adjusted_tag_count_created_at_year",
        "created_at_year_per_tag_count_created_at_year",
        "created_at_year_num_cume_dist_per_created_at_year_per_tag_count_created_at_year",
    )
    .orderBy(F.col("created_at_year_per_tag_count_created_at_year").desc(), "tag")
)
