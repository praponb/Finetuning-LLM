from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("k").orderBy(F.col("created_at_weekofyear_whole").asc_nulls_first())
w3 = Window.partitionBy("k")
w4 = Window.partitionBy("k").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

filtered = (
    flattened
    .filter(F.col("k").isNotNull())
    .withColumn("is_bundle", F.array_contains("coupon_codes", "BUNDLE"))
    .withColumn("created_at_weekofyear", F.weekofyear("created_at"))
    .withColumn("k_filled", F.coalesce("k", F.lit("NONE")))
)

base = (
    filtered
    .filter(F.col("k").isNotNull() & F.col("is_bundle"))
    .select(
        "*",
        F.floor("created_at_weekofyear").alias("created_at_weekofyear_whole"),
        F.coalesce("k", F.lit("OTHER")).alias("k_clean"),
    )
)

scored = (
    base
    .withColumn("created_at_weekofyear_sum_2h", F.sum("created_at_weekofyear").over(w))
    .withColumn("created_at_weekofyear_whole_rank", F.rank().over(w2))
)

metrics = (
    scored
    .select(
        "*",
        F.max("created_at_weekofyear_whole").over(w3).alias("k_max_created_at_weekofyear_whole"),
        F.avg("created_at_weekofyear").over(w4).alias("created_at_weekofyear_avg_1h"),
        F.count("created_at_weekofyear_whole").over(w).alias("created_at_weekofyear_whole_count_2h"),
    )
)

report = (
    metrics
    .select(
        "k",
        "v",
        "is_bundle",
        "created_at_weekofyear",
        "created_at_weekofyear_whole",
        "created_at_weekofyear_sum_2h",
        "created_at_weekofyear_whole_rank",
        "k_max_created_at_weekofyear_whole",
        "created_at_weekofyear_avg_1h",
        F.when(F.col("k").rlike("^(utm_source|device)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("k_zone"),
    )
    .orderBy(F.col("created_at_weekofyear_whole").desc_nulls_last())
)

result = (
    report
    .select(
        "is_bundle",
        "created_at_weekofyear_whole",
        "created_at_weekofyear_sum_2h",
        "k_max_created_at_weekofyear_whole",
        "created_at_weekofyear_avg_1h",
    )
    .orderBy("created_at_weekofyear_whole", "k_max_created_at_weekofyear_whole")
)
