from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("val"))
)

result = (
    lines
    .withColumn("currency_max_val", F.max("val").over(w))
    .withColumn("currency_min_val", F.min("val").over(w))
    .withColumn("currency_sum_val", F.sum("val").over(w))
)
