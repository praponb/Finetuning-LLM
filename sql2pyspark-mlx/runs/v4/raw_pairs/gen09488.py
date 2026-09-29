from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("tier_value")
w2 = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

filtered = (
    lines
    .filter(F.col("currency").isin(["EUR", "USD"]))
    .select("*", F.concat("currency", F.lit(" - "), "plan").alias("currency_plan"))
)

metrics = (
    filtered
    .withColumn("tier_value_rank", F.rank().over(w))
    .withColumn("currency_stddev_samp_tier_value", F.stddev_samp("tier_value").over(w2))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("tier_value") >= 70, "HIGH").otherwise("LOW").alias("tier_value_tier"),
    )
    .agg(
        F.round(F.sum("currency_stddev_samp_tier_value") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("currency_stddev_samp_tier_value_per_pro"),
        F.round(F.avg("tier_value"), 2).alias("avg_tier_value"),
    )
    .orderBy(F.col("tier_value_tier").desc())
)
