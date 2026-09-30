from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").asc_nulls_first())
w2 = Window.partitionBy("plan").orderBy("val")
w3 = Window.partitionBy("plan")

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_limits").alias("val"))
)

valid = (
    lines
    .filter(F.col("val") <= 29)
    .withColumn("effective_from_month_end", F.last_day("effective_from"))
)

cleaned = (
    valid
    .filter(F.col("effective_from") <= "2024-04-01")
    .select("*", F.substring("plan", 1, 3).alias("plan_prefix"))
)

with_windows = (
    cleaned
    .filter(F.col("plan").isin(["pro", "enterprise"]))
    .withColumn("val_cume_dist", F.cume_dist().over(w))
    .withColumn("val_rank", F.rank().over(w2))
    .withColumn("plan_avg_val", F.avg("val").over(w3))
)

result = (
    with_windows
    .select("plan", "val", "plan_prefix", "val_cume_dist", "val_rank")
    .orderBy(F.col("val").desc(), "plan")
)
