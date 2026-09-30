from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("tier_value_whole").asc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode("tier_limits").alias("tier_value"),
    )
)

valid = (
    lines
    .filter(F.year("effective_from") == 2023)
    .withColumn("plan_title", F.initcap(F.trim("plan")))
)

base = (
    valid
    .filter(F.col("effective_from") > "2024-06-01")
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_value",
        "plan_title",
        F.floor("tier_value").alias("tier_value_whole"),
    )
)

scored = (
    base
    .withColumn("plan_avg_tier_value_whole", F.avg("tier_value_whole").over(w))
    .withColumn("tier_value_whole_percent_rank", F.percent_rank().over(w2))
)

result = (
    scored
    .select("id", "plan", "effective_from", "tier_value", "plan_title")
    .orderBy(F.col("plan").desc(), F.col("id").desc())
)
