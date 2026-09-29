from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("tier_value").desc())
w_recent_2 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("tier_value").desc_nulls_first())
w5 = Window.orderBy(F.col("plan_max_tier_value").asc_nulls_last())
w6 = Window.partitionBy("plan").orderBy(F.col("plan_avg_tier_value_plus_tier_value_cume_dist").asc_nulls_first())

exploded = (
    df_price_tiers
    .select("id", "plan", F.explode_outer("discounts").alias("tier_value"))
)

scored = (
    exploded
    .select(
        "id",
        "plan",
        "tier_value",
        F.avg("tier_value").over(w_recent).alias("plan_avg_tier_value"),
        F.rank().over(w2).alias("tier_value_rank"),
        F.cume_dist().over(w_recent_2).alias("tier_value_cume_dist"),
        F.max("tier_value").over(w_recent).alias("plan_max_tier_value"),
        F.cume_dist().over(w4).alias("tier_value_cume_dist_2"),
    )
)

base = (
    scored
    .filter(F.col("plan") != "pro")
    .withColumn("plan_avg_tier_value_plus_tier_value_cume_dist", (F.col("plan_avg_tier_value") + F.col("tier_value_cume_dist")))
)

filtered = (
    base
    .filter(F.col("tier_value").between(30, 54))
    .select(
        "id",
        "plan",
        "tier_value",
        "plan_max_tier_value",
        "tier_value_cume_dist_2",
        "plan_avg_tier_value_plus_tier_value_cume_dist",
        (F.col("tier_value_cume_dist") * F.col("plan_avg_tier_value")).alias("tier_value_cume_dist_times_plan_avg_tier_value"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.min("plan_max_tier_value").over(w_recent).alias("plan_min_plan_max_tier_value"),
        F.cume_dist().over(w5).alias("plan_max_tier_value_cume_dist"),
        F.percent_rank().over(w6).alias("plan_avg_tier_value_plus_tier_value_cume_dist_percent_rank"),
        F.min("tier_value_cume_dist_times_plan_avg_tier_value").over(w_recent).alias("plan_min_tier_value_cume_dist_times_plan_avg_tier_value"),
        F.min("tier_value_cume_dist_2").over(w_recent).alias("plan_min_tier_value_cume_dist_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "plan",
        "tier_value",
        "plan_max_tier_value",
        "plan_min_plan_max_tier_value",
        "plan_max_tier_value_cume_dist",
        "plan_avg_tier_value_plus_tier_value_cume_dist_percent_rank",
        "plan_min_tier_value_cume_dist_times_plan_avg_tier_value",
    )
    .orderBy(F.col("plan_max_tier_value").desc())
)
