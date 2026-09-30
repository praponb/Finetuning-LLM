from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_first())
w3 = Window.orderBy(F.col("element_int").desc_nulls_first())
w4 = Window.orderBy(F.col("plan_stddev_samp_element").asc_nulls_last())
w5 = Window.partitionBy("plan").orderBy("element_cume_dist")
w6 = Window.partitionBy("plan").orderBy(F.col("element_int").asc_nulls_last())
w7 = Window.partitionBy("plan").orderBy("plan_sum_plan_max_element")
w8 = Window.orderBy(F.col("plan_max_element").asc_nulls_last())
w9 = Window.orderBy(F.col("plan_sum_plan_max_element").desc())
w10 = Window.partitionBy("year_start").orderBy(F.col("element_cume_dist_rank_per_pro").desc())

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("discounts").alias("element"))
)

cleaned = (
    flattened
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-08-28")))
    .select("*", F.col("element").cast("int").alias("element_int"))
)

metrics = (
    cleaned
    .withColumn("plan_stddev_samp_element", F.stddev_samp("element").over(w))
    .withColumn("element_cume_dist", F.cume_dist().over(w2))
    .withColumn("plan_max_element", F.max("element").over(w))
    .withColumn("plan_stddev_samp_element_2", F.stddev_samp("element").over(w))
    .withColumn("element_int_cume_dist", F.cume_dist().over(w3))
)

metrics_price_tiers = (
    metrics
    .select(
        "*",
        F.max("element_int").over(w).alias("plan_max_element_int"),
        F.cume_dist().over(w4).alias("plan_stddev_samp_element_cume_dist"),
        F.sum("plan_max_element").over(w).alias("plan_sum_plan_max_element"),
        F.rank().over(w5).alias("element_cume_dist_rank"),
        F.rank().over(w6).alias("element_int_rank"),
    )
)

enriched = (
    metrics_price_tiers
    .select(
        "*",
        F.avg("element").over(w).alias("plan_avg_element"),
        F.dense_rank().over(w7).alias("plan_sum_plan_max_element_dense_rank"),
        F.dense_rank().over(w8).alias("plan_max_element_dense_rank"),
        F.dense_rank().over(w9).alias("plan_sum_plan_max_element_dense_rank_2"),
    )
)

result = (
    enriched
    .groupBy(
        F.date_trunc("YEAR", "effective_from").alias("year_start"),
        "plan",
        F.date_trunc("QUARTER", "effective_from").alias("quarter_start"),
    )
    .agg(
        F.countDistinct("plan").alias("distinct_plans"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.round(F.sum("element_cume_dist_rank") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("element_cume_dist_rank_per_pro"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.round(F.sum("element_int_rank") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("element_int_rank_per_pro"),
        F.percentile_approx("element_int_rank", [0.25, 0.5, 0.75]).alias("element_int_rank_quartiles"),
    )
    .filter(F.col("num_plans") > 1)
    .withColumn("element_cume_dist_rank_per_pro_percent_rank", F.percent_rank().over(w10))
)
