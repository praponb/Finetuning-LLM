from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc_nulls_first())
w2 = Window.orderBy(F.col("val").desc())
w3 = Window.orderBy(F.col("val").desc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("val"))
)

result = (
    exploded
    .withColumn("val_percent_rank", F.percent_rank().over(w))
    .withColumn("val_cume_dist", F.cume_dist().over(w2))
    .withColumn("val_rank", F.rank().over(w3))
)
