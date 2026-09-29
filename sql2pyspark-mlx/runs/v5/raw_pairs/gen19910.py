from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code").orderBy(F.col("is_spring10").asc_nulls_last())
w2 = Window.partitionBy("code")

exploded = df_carts.select("id", F.explode("coupon_codes").alias("code"))

cleaned = (
    exploded
    .filter(F.col("code").isNotNull() & (~F.col("code").eqNullSafe(F.lit("VIP"))))
    .select(
        "*",
        F.when(F.col("code") == "SPRING10", 1).otherwise(0).alias("is_spring10"),
        F.coalesce("code", F.lit("UNKNOWN")).alias("code_filled"),
    )
)

cleaned_carts = (
    cleaned
    .filter(F.col("code").isNotNull())
    .select(
        "*",
        F.floor("is_spring10").alias("is_spring10_whole"),
        F.round((F.col("is_spring10") * 100), 2).alias("is_spring10_adjusted"),
    )
)

with_windows = (
    cleaned_carts
    .select(
        "id",
        "code",
        "code_filled",
        "is_spring10_adjusted",
        F.rank().over(w).alias("is_spring10_rank"),
        F.min("is_spring10_whole").over(w2).alias("code_min_is_spring10_whole"),
        F.count("is_spring10_whole").over(w2).alias("code_count_is_spring10_whole"),
    )
)

with_windows_carts = (
    with_windows
    .withColumn("code_sum_is_spring10_rank", F.sum("is_spring10_rank").over(w2))
)

result = (
    with_windows_carts
    .withColumn("code_min_is_spring10_whole_tier", F.when(F.col("code_min_is_spring10_whole") >= 86, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["code", "code_min_is_spring10_whole_tier"], ["code"]],
        "code",
        "code_min_is_spring10_whole_tier",
    )
    .agg(
        F.percentile_approx("code_count_is_spring10_whole", [0.25, 0.5, 0.75]).alias("code_count_is_spring10_whole_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("code_min_is_spring10_whole", [0.25, 0.5, 0.75]).alias("code_min_is_spring10_whole_quartiles"),
        F.grouping("code").alias("gid"),
    )
    .filter(F.col("distinct_ids") < 1000)
)
