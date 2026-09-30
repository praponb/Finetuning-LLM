from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc())
w2 = Window.partitionBy("plan")

exploded = df_price_tiers.select("id", "plan", F.explode_outer("discounts").alias("val"))

result = (
    exploded
    .withColumn("val_dense_rank", F.dense_rank().over(w))
    .withColumn("plan_max_val", F.max("val").over(w2))
)
