from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("plan", "currency")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("val"),
    )
)

result = (
    lines
    .withColumn("currency_stddev_samp_val", F.stddev_samp("val").over(w))
    .withColumn("plan_min_val", F.min("val").over(w2))
)
