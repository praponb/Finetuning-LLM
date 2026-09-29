from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy("val")
w3 = Window.partitionBy("currency")

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("val"),
    )
)

result = (
    exploded
    .select(
        "*",
        F.max("val").over(w).alias("plan_max_val"),
        F.rank().over(w2).alias("val_rank"),
        F.avg("val").over(w3).alias("currency_avg_val"),
        F.stddev_samp("val").over(w3).alias("currency_stddev_samp_val"),
        F.stddev_samp("val").over(w).alias("plan_stddev_samp_val"),
    )
)
