from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy("plan_length")
w3 = Window.orderBy("plan_length")
w4 = Window.partitionBy("plan").orderBy(F.col("plan_length").asc_nulls_last())
w5 = Window.partitionBy("plan").orderBy("plan_stddev_samp_plan_length")
w6 = Window.orderBy(F.col("plan_stddev_samp_plan_length").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

valid = (
    flattened
    .filter(F.col("element") >= 80)
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "element",
        F.length("plan").alias("plan_length"),
    )
)

cleaned = (
    valid
    .filter(F.col("currency") == "THB")
    .select(
        "id",
        "plan",
        "effective_from",
        "plan_length",
        F.lower("plan").alias("plan_lower"),
    )
)

metrics = (
    cleaned
    .withColumn("plan_stddev_samp_plan_length", F.stddev_samp("plan_length").over(w))
    .withColumn("plan_length_rank", F.rank().over(w2))
    .withColumn("plan_length_cume_dist", F.cume_dist().over(w3))
    .withColumn("plan_length_rank_2", F.rank().over(w4))
    .withColumn("plan_length_cume_dist_2", F.cume_dist().over(w4))
)

filtered = (
    metrics
    .filter((F.col("effective_from") > F.lit("2024-02-27").cast("date")) & (F.col("plan_stddev_samp_plan_length") == 67))
    .select(
        "*",
        F.ceil("plan_length").alias("plan_length_whole"),
        (F.col("plan_length") + F.col("plan_length_cume_dist")).alias("plan_length_plus_plan_length_cume_dist"),
    )
)

result = (
    filtered
    .filter(F.col("plan_stddev_samp_plan_length") < 36)
    .select(
        "plan",
        "effective_from",
        "plan_stddev_samp_plan_length",
        "plan_length_cume_dist",
        "plan_length_rank_2",
        "plan_length_cume_dist_2",
        F.percent_rank().over(w5).alias("plan_stddev_samp_plan_length_percent_rank"),
        F.stddev_samp("plan_length_rank_2").over(w).alias("plan_stddev_samp_plan_length_rank_2"),
        F.max("plan_length_rank").over(w).alias("plan_max_plan_length_rank"),
        F.cume_dist().over(w6).alias("plan_stddev_samp_plan_length_cume_dist"),
        F.sum("plan_length_plus_plan_length_cume_dist").over(w).alias("plan_sum_plan_length_plus_plan_length_cume_dist"),
    )
)
