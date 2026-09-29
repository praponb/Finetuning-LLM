from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_first())
w3 = Window.partitionBy("currency", "plan").orderBy(F.col("element").desc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

result = (
    lines
    .filter(F.col("plan").isin(["enterprise", "starter", "basic"]))
    .select(
        "id",
        "plan",
        F.stddev_samp("element").over(w_recent).alias("plan_stddev_samp_element"),
        F.percent_rank().over(w2).alias("element_percent_rank"),
        F.cume_dist().over(w3).alias("element_cume_dist"),
    )
)
