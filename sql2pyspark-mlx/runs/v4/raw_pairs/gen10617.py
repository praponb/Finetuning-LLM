from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tier_value_tier").orderBy(F.col("distinct_plans").desc())

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("tier_value"))
)

result = (
    exploded
    .groupBy(
        F.when(F.col("tier_value") >= 61, "LARGE").otherwise("SMALL").alias("tier_value_tier"),
        "currency",
    )
    .agg(
        F.countDistinct("plan").alias("distinct_plans"),
        F.round(F.sum("tier_value") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("tier_value_per_basic"),
        F.percentile_approx("tier_value", 0.5).alias("tier_value_p50"),
    )
    .withColumn("distinct_plans_rank", F.rank().over(w))
    .orderBy(F.col("tier_value_tier").desc())
)
