from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("val").asc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("val"))
)

result = (
    exploded
    .withColumn("currency_sum_val", F.sum("val").over(w))
    .withColumn("val_rank", F.rank().over(w2))
)
