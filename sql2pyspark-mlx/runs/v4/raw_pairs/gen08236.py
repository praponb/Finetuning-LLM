from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("val").desc_nulls_last())
w3 = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("tier_limits").alias("val"))
)

result = (
    lines
    .withColumn("plan_avg_val", F.avg("val").over(w))
    .withColumn("val_percent_rank", F.percent_rank().over(w2))
    .withColumn("val_rank", F.rank().over(w3))
    .withColumn("plan_max_val", F.max("val").over(w))
    .withColumn("val_rank_2", F.rank().over(w2))
)
