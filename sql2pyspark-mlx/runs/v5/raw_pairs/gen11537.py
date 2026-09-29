from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_first())
w_part = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_limits").alias("element"))
)

result = (
    flattened
    .select(
        "id",
        "effective_from",
        "element",
        F.stddev_samp("element").over(w_running).alias("plan_stddev_samp_element"),
        F.count("element").over(w_running).alias("plan_count_element"),
        F.avg("element").over(w_running).alias("plan_avg_element"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
        F.cume_dist().over(w_part).alias("element_cume_dist"),
    )
)
