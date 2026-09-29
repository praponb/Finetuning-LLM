from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_first())
w_ordered = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_first())
w4 = Window.partitionBy("plan").orderBy(F.col("element_percent_rank").desc())
w5 = Window.partitionBy("plan").orderBy(F.col("plan_count_element").desc_nulls_last())
w6 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_limits").alias("element"))
)

scored = (
    flattened
    .select(
        "*",
        F.count("element").over(w_running).alias("plan_count_element"),
        F.percent_rank().over(w2).alias("element_percent_rank"),
        F.stddev_samp("element").over(w_running).alias("plan_stddev_samp_element"),
        F.cume_dist().over(w_ordered).alias("element_cume_dist"),
        F.count("element").over(w_running).alias("plan_count_element_2"),
    )
)

with_windows = (
    scored
    .filter(F.col("plan_count_element").isNull())
    .select(
        "id",
        "plan",
        "element",
        "plan_count_element",
        "element_percent_rank",
        "plan_stddev_samp_element",
        F.max("plan_stddev_samp_element").over(w_running).alias("plan_max_plan_stddev_samp_element"),
        F.dense_rank().over(w4).alias("element_percent_rank_dense_rank"),
    )
)

matched_keys = (
    with_windows
    .select("plan", "id")
    .intersectAll(
        df_price_tiers
        .filter(F.col("plan") == "basic")
        .select("plan", "id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("plan_count_element") < 14)
    .select(
        "*",
        F.least("plan_count_element", "plan_max_plan_stddev_samp_element").alias("least_plan_count_element_plan_max_plan_stddev_samp_element"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "element",
        "plan_count_element",
        "plan_stddev_samp_element",
        "plan_max_plan_stddev_samp_element",
        "least_plan_count_element_plan_max_plan_stddev_samp_element",
        F.rank().over(w5).alias("plan_count_element_rank"),
        F.count("element_percent_rank").over(w_running).alias("plan_count_element_percent_rank"),
        F.stddev_samp("plan_count_element").over(w_running).alias("plan_stddev_samp_plan_count_element"),
        F.cume_dist().over(w6).alias("element_cume_dist"),
    )
)

base = (
    enriched
    .filter(F.col("plan_count_element_percent_rank") == 12)
    .withColumn("plan_stddev_samp_element_whole", F.ceil("plan_stddev_samp_element"))
)

result = (
    base
    .groupBy(
        F.when(F.col("plan_stddev_samp_element") >= 83, "PREMIUM").otherwise("STANDARD").alias("plan_stddev_samp_element_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("element_cume_dist", 0.95).alias("element_cume_dist_p95"),
    )
    .orderBy(F.col("cnt").desc())
)
