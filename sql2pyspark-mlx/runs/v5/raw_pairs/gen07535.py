from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy("element")
w3 = Window.orderBy(F.col("element_percent_rank").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_limits").alias("element"),
    )
)

filtered = (
    lines
    .filter(F.col("element").isNotNull())
    .withColumn("element_num", F.col("element").cast("double"))
)

enriched = (
    filtered
    .filter(~F.col("plan").isin(["pro", "starter", "enterprise"]))
    .select(
        "id",
        "plan",
        "element",
        "element_num",
        F.stddev_samp("element_num").over(w_ordered).alias("plan_stddev_samp_element_num"),
        F.avg("element").over(w_ordered).alias("plan_avg_element"),
        F.percent_rank().over(w2).alias("element_percent_rank"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "plan",
        "plan_stddev_samp_element_num",
        "plan_avg_element",
        "element_percent_rank",
        F.sum("element_percent_rank").over(w_ordered).alias("plan_sum_element_percent_rank"),
        F.cume_dist().over(w3).alias("element_percent_rank_cume_dist"),
    )
)

result = (
    scored
    .withColumn("plan_sum_element_percent_rank_tier", F.when(F.col("plan_sum_element_percent_rank") >= 3, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["plan", "plan_sum_element_percent_rank_tier"], ["plan"], []],
        "plan",
        "plan_sum_element_percent_rank_tier",
    )
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_count"),
        F.percentile_approx("plan_stddev_samp_element_num", [0.25, 0.5, 0.75]).alias("plan_stddev_samp_element_num_quartiles"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows_2"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_rows"),
        F.grouping_id().alias("grouping_id"),
    )
)
