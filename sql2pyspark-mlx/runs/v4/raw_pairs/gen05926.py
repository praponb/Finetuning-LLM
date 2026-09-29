from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy("val")
w3 = Window.orderBy(F.col("val").desc())

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("discounts").alias("val"))
)

result = (
    exploded
    .withColumn("plan_max_val", F.max("val").over(w))
    .withColumn("val_dense_rank", F.dense_rank().over(w2))
    .withColumn("val_cume_dist", F.cume_dist().over(w3))
)
