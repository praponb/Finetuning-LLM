from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("element").desc())
w3 = Window.orderBy("element")
w4 = Window.partitionBy("plan", "currency")
w5 = Window.partitionBy("currency")

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("element"))
)

enriched = (
    exploded
    .withColumn("plan_max_element", F.max("element").over(w_recent))
    .withColumn("element_cume_dist", F.cume_dist().over(w2))
    .withColumn("plan_stddev_samp_element", F.stddev_samp("element").over(w_recent))
    .withColumn("element_rank", F.rank().over(w3))
    .withColumn("plan_stddev_samp_element_2", F.stddev_samp("element").over(w4))
)

filtered = (
    enriched
    .filter(F.col("plan_stddev_samp_element_2") >= 44)
    .select(
        "id",
        "plan",
        "currency",
        "element",
        "plan_max_element",
        "element_cume_dist",
        "element_rank",
        "plan_stddev_samp_element_2",
        (F.col("plan_stddev_samp_element_2") * F.col("element")).alias("plan_stddev_samp_element_2_times_element"),
        (F.col("element_cume_dist") + F.col("plan_stddev_samp_element")).alias("element_cume_dist_plus_plan_stddev_samp_element"),
    )
)

result = (
    filtered
    .withColumn("currency_count_plan_stddev_samp_element_2_times_element", F.count("plan_stddev_samp_element_2_times_element").over(w5))
    .withColumn("plan_max_plan_stddev_samp_element_2", F.max("plan_stddev_samp_element_2").over(w_recent))
)
