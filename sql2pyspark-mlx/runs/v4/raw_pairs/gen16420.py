from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_first())
w2 = Window.partitionBy("currency").orderBy("val")

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("val"))
)

result = (
    exploded
    .withColumn("val_rank", F.rank().over(w))
    .withColumn("val_rank_2", F.rank().over(w2))
)
