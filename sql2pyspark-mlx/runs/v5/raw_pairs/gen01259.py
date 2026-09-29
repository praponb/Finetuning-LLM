from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_first())
w2 = Window.orderBy(F.col("val").desc_nulls_first())

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("tier_limits").alias("val"))
)

result = (
    flattened
    .withColumn("val_rank", F.rank().over(w))
    .withColumn("val_rank_2", F.rank().over(w2))
)
