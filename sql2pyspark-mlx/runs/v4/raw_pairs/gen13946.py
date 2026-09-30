from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("val")
w2 = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_prices").alias("val"))
)

result = (
    flattened
    .withColumn("val_cume_dist", F.cume_dist().over(w))
    .withColumn("plan_stddev_samp_val", F.stddev_samp("val").over(w2))
    .withColumn("val_percent_rank", F.percent_rank().over(w))
)
