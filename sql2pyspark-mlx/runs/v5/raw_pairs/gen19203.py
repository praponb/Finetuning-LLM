from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("currency", "plan").orderBy(F.col("tier_value").desc_nulls_first())
w_hist = Window.partitionBy("plan")

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("tier_value"))
)

scored = (
    exploded
    .select(
        "plan",
        "tier_value",
        F.cume_dist().over(w_running).alias("tier_value_cume_dist"),
    )
)

enriched = (
    scored
    .filter(F.col("tier_value") <= 27)
    .withColumn("plan_stddev_samp_tier_value_cume_dist", F.stddev_samp("tier_value_cume_dist").over(w_hist))
)

filtered = (
    enriched
    .filter(F.col("tier_value_cume_dist") <= 0.34)
    .select("*", F.length("plan").alias("plan_length"))
)

base = (
    filtered
    .filter(F.col("tier_value_cume_dist") > 0.33)
    .select(
        "*",
        F.least("plan_length", "tier_value").alias("least_plan_length_tier_value"),
        (F.col("plan_length") * F.col("tier_value_cume_dist")).alias("plan_length_times_tier_value_cume_dist"),
    )
)

result = (
    base
    .select("plan", "tier_value", "plan_stddev_samp_tier_value_cume_dist")
    .orderBy(F.col("plan_stddev_samp_tier_value_cume_dist").desc_nulls_last(), "plan")
)
