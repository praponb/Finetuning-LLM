from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").asc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("tier_limits").alias("tier_value"))
)

result = (
    exploded
    .withColumn("plan_stddev_samp_tier_value", F.stddev_samp("tier_value").over(w_recent))
    .withColumn("plan_max_tier_value", F.max("tier_value").over(w_recent))
    .withColumn("plan_stddev_samp_tier_value_2", F.stddev_samp("tier_value").over(w_recent))
    .withColumn("tier_value_cume_dist", F.cume_dist().over(w2))
)
