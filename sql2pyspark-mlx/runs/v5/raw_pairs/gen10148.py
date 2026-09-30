from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("currency").orderBy(F.col("tier_value").asc_nulls_first())
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc())
w3 = Window.partitionBy("currency", "plan")
w_ordered = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "plan",
        "tier_value",
        F.percent_rank().over(w_grp).alias("tier_value_percent_rank"),
        F.rank().over(w2).alias("tier_value_rank"),
        F.stddev_samp("tier_value").over(w3).alias("currency_stddev_samp_tier_value"),
        F.min("tier_value").over(w_ordered).alias("plan_min_tier_value"),
    )
)
