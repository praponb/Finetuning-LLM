from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_first())
w2 = Window.orderBy(F.col("element").desc())
w3 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("element_dense_rank").asc_nulls_last())
w5 = Window.partitionBy("plan").orderBy("element_cume_dist")
w6 = Window.partitionBy("plan")

lines = (
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
    lines
    .filter(F.col("element").between(23, 58))
    .select(
        "id",
        "plan",
        "effective_from",
        "element",
        F.initcap(F.trim("plan")).alias("plan_title"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.dense_rank().over(w).alias("element_dense_rank"),
        F.cume_dist().over(w2).alias("element_cume_dist"),
        F.rank().over(w3).alias("element_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.percent_rank().over(w4).alias("element_dense_rank_percent_rank"),
        F.dense_rank().over(w5).alias("element_cume_dist_dense_rank"),
        F.max("element").over(w6).alias("plan_max_element"),
    )
)

with_windows_price_tiers = (
    metrics
    .withColumn("element_dense_rank_cume_dist", F.cume_dist().over(w4))
    .withColumn("element_cume_dist_2", F.cume_dist().over(w3))
)

summary = (
    with_windows_price_tiers
    .groupBy("plan")
    .agg(
        F.sum(F.when(F.col("plan") == "pro", F.col("plan_max_element")).otherwise(0)).alias("pro_plan_max_element"),
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_count"),
        F.sum(F.when(F.col("plan") == "starter", F.col("element_cume_dist_2"))).alias("starter_total_element_cume_dist_2"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    summary
    .select(
        "plan",
        "pro_plan_max_element",
        "basic_events",
        "starter_total_element_cume_dist_2",
        "enterprise_count",
        "distinct_ids",
    )
    .orderBy(F.col("distinct_ids").desc_nulls_first())
)
