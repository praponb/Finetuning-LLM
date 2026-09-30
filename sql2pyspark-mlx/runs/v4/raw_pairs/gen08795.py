from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("val")
w2 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_first())
w3 = Window.partitionBy("plan")

exploded = df_price_tiers.select("id", "plan", F.explode_outer("discounts").alias("val"))

result = (
    exploded
    .select(
        "*",
        F.percent_rank().over(w).alias("val_percent_rank"),
        F.percent_rank().over(w2).alias("val_percent_rank_2"),
        F.stddev_samp("val").over(w3).alias("plan_stddev_samp_val"),
    )
)
