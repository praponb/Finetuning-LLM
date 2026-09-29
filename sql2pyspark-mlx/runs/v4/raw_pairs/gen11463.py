from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_first())
w3 = Window.partitionBy("plan").orderBy(F.col("plan_avg_tier_value_2").desc_nulls_first())

exploded = df_price_tiers.select("id", "plan", F.explode("discounts").alias("tier_value"))

base = (
    exploded
    .filter(F.col("plan").isin(["starter", "enterprise", "basic"]))
    .select(
        "*",
        F.col("tier_value").cast("int").alias("tier_value_int"),
        F.upper("plan").alias("plan_upper"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.avg("tier_value").over(w).alias("plan_avg_tier_value"),
        F.avg("tier_value").over(w).alias("plan_avg_tier_value_2"),
        F.sum("tier_value_int").over(w).alias("plan_sum_tier_value_int"),
        F.percent_rank().over(w2).alias("tier_value_percent_rank"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.percent_rank().over(w3).alias("plan_avg_tier_value_2_percent_rank"),
        F.max("tier_value_int").over(w).alias("plan_max_tier_value_int"),
        F.max("tier_value").over(w).alias("plan_max_tier_value"),
    )
)

result = (
    scored
    .select(
        "id",
        "tier_value",
        "tier_value_int",
        "plan_upper",
        "plan_avg_tier_value",
        "plan_avg_tier_value_2",
        "tier_value_percent_rank",
        "plan_avg_tier_value_2_percent_rank",
        "plan_max_tier_value",
    )
    .orderBy(
        F.col("plan_max_tier_value").asc_nulls_last(),
        F.col("plan_avg_tier_value").desc(),
    )
)
