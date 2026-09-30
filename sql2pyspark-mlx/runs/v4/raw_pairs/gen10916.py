from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").desc())
w2 = Window.orderBy(F.col("element").desc_nulls_last())
w3 = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_last())
w4 = Window.orderBy(F.col("element").asc_nulls_first())
w_ordered = Window.partitionBy("plan").orderBy(F.col("element_percent_rank").desc_nulls_first())
w6 = Window.partitionBy("plan")
w7 = Window.partitionBy("plan").orderBy(F.col("element_cume_dist_2").asc_nulls_first())
w8 = Window.orderBy(F.col("element_percent_rank_dense_rank").asc_nulls_last())
w9 = Window.partitionBy("plan").orderBy(F.col("element_cume_dist_2_cume_dist").desc_nulls_first())
w10 = Window.partitionBy("plan").orderBy(F.col("plan_count_element_cume_dist").desc_nulls_first())

exploded = df_price_tiers.select("id", "plan", F.explode("tier_limits").alias("element"))

with_windows = (
    exploded
    .select(
        "*",
        F.rank().over(w).alias("element_rank"),
        F.cume_dist().over(w2).alias("element_cume_dist"),
        F.cume_dist().over(w3).alias("element_cume_dist_2"),
        F.dense_rank().over(w).alias("element_dense_rank"),
        F.percent_rank().over(w4).alias("element_percent_rank"),
    )
)

enriched = (
    with_windows
    .withColumn("element_percent_rank_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("plan_count_element_cume_dist", F.count("element_cume_dist").over(w6))
)

with_windows_price_tiers = (
    enriched
    .withColumn("plan_min_element_dense_rank", F.min("element_dense_rank").over(w6))
    .withColumn("element_cume_dist_2_cume_dist", F.cume_dist().over(w7))
    .withColumn("element_percent_rank_dense_rank_rank", F.rank().over(w8))
    .withColumn("plan_count_element_dense_rank", F.count("element_dense_rank").over(w6))
    .withColumn("element_dense_rank_share", F.round((F.col("element_dense_rank") / F.sum("element_dense_rank").over(w6)), 4))
)

metrics = (
    with_windows_price_tiers
    .withColumn("element_cume_dist_2_cume_dist_percent_rank", F.percent_rank().over(w9))
    .withColumn("plan_count_element", F.count("element").over(w6))
    .withColumn("plan_count_element_cume_dist_dense_rank", F.dense_rank().over(w10))
)

filtered = (
    metrics
    .filter(F.col("element_cume_dist_2") <= 0.27)
    .select(
        "id",
        "plan",
        "element",
        "element_rank",
        "element_cume_dist",
        "element_cume_dist_2",
        "element_dense_rank",
        "element_percent_rank",
        "element_percent_rank_dense_rank",
        "plan_count_element_cume_dist",
        "plan_min_element_dense_rank",
        "element_cume_dist_2_cume_dist",
        "element_percent_rank_dense_rank_rank",
        "plan_count_element_dense_rank",
        "element_dense_rank_share",
        "element_cume_dist_2_cume_dist_percent_rank",
        "plan_count_element",
        "plan_count_element_cume_dist_dense_rank",
        F.when(F.col("element_cume_dist") >= 0.81, "High").when(F.col("element_cume_dist") >= 0.18, "Medium").otherwise("Low").alias("element_cume_dist_level"),
        (F.col("element_percent_rank_dense_rank_rank") - F.col("element_percent_rank")).alias("element_percent_rank_dense_rank_rank_minus_element_percent_rank"),
    )
)

result = (
    filtered
    .select(
        "element",
        "element_rank",
        "element_dense_rank",
        "element_percent_rank_dense_rank",
        "element_cume_dist_2_cume_dist",
        "element_percent_rank_dense_rank_rank",
        "plan_count_element_dense_rank",
        "element_dense_rank_share",
        "element_cume_dist_2_cume_dist_percent_rank",
    )
    .orderBy(F.col("element_rank").desc_nulls_last(), "element_cume_dist_2_cume_dist")
)
