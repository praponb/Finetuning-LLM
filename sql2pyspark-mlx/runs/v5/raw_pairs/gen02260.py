from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("element").desc())
w3 = Window.partitionBy("plan").orderBy("element_whole")
w4 = Window.orderBy(F.col("element_whole_rank").desc_nulls_first())
w5 = Window.partitionBy("plan").orderBy(F.col("plan_avg_element_whole").desc_nulls_last())
w6 = Window.orderBy("element")
w7 = Window.partitionBy("plan").orderBy(F.col("element_whole_rank_percent_rank").desc())

exploded = df_price_tiers.select("id", "plan", F.explode("discounts").alias("element"))

filtered = (
    exploded
    .filter(F.col("plan") != "basic")
    .withColumn("element_whole", F.floor("element"))
)

cleaned = (
    filtered
    .filter(F.col("element") > 80)
    .withColumn("element_whole_minus_element", (F.col("element_whole") - F.col("element")))
)

enriched = (
    cleaned
    .filter(F.col("element_whole_minus_element") >= 53)
    .select(
        "*",
        F.max("element_whole_minus_element").over(w).alias("plan_max_element_whole_minus_element"),
        F.avg("element_whole").over(w).alias("plan_avg_element_whole"),
        F.sum("element_whole").over(w).alias("plan_sum_element_whole"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
        F.rank().over(w3).alias("element_whole_rank"),
    )
)

with_windows = (
    enriched
    .withColumn("element_whole_rank_percent_rank", F.percent_rank().over(w4))
    .withColumn("plan_stddev_samp_plan_avg_element_whole", F.stddev_samp("plan_avg_element_whole").over(w))
    .withColumn("plan_avg_element_whole_cume_dist", F.cume_dist().over(w5))
)

scored = (
    with_windows
    .withColumn("element_rank", F.rank().over(w6))
    .withColumn("element_whole_rank_percent_rank_percent_rank", F.percent_rank().over(w7))
)

result = (
    scored
    .select(
        "id",
        "plan",
        "element",
        "element_whole",
        "plan_max_element_whole_minus_element",
        "plan_avg_element_whole",
        "plan_sum_element_whole",
        "element_dense_rank",
        "plan_stddev_samp_plan_avg_element_whole",
    )
    .orderBy("id")
)
