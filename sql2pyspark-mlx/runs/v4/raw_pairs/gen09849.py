from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy("val")
w2 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w3 = Window.partitionBy("currency")
w4 = Window.orderBy(F.col("val").desc_nulls_last())
w5 = Window.partitionBy("plan").orderBy("val")

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("val"))
)

result = (
    flattened
    .withColumn("val_rank", F.rank().over(w))
    .withColumn("val_percent_rank", F.percent_rank().over(w2))
    .withColumn("currency_avg_val", F.avg("val").over(w3))
    .withColumn("val_percent_rank_2", F.percent_rank().over(w4))
    .withColumn("val_rank_2", F.rank().over(w5))
)
