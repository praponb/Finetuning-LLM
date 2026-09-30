from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")

flattened = df_price_tiers.select("id", "plan", F.explode("tier_prices").alias("tier_value"))

cleaned = (
    flattened
    .filter(F.col("tier_value") > 31)
    .withColumn("tier_value_band", F.when(F.col("tier_value") >= 75, "High").when(F.col("tier_value") >= 26, "Medium").otherwise("Low"))
)

valid = (
    cleaned
    .filter(F.col("tier_value") <= 65)
    .select(
        "*",
        F.when(F.col("plan") == "pro", 1).otherwise(0).alias("is_pro"),
        F.when(F.col("plan") == "starter", 1).otherwise(0).alias("is_starter"),
    )
)

base = (
    valid
    .filter(F.col("plan") == "starter")
    .withColumn("tier_value_int", F.col("tier_value").cast("int"))
)

enriched = (
    base
    .withColumn("plan_max_tier_value_int", F.max("tier_value_int").over(w))
    .withColumn("plan_avg_is_pro", F.avg("is_pro").over(w))
    .withColumn("plan_stddev_samp_is_pro", F.stddev_samp("is_pro").over(w))
)

result = (
    enriched
    .groupBy("plan")
    .agg(
        F.percentile_approx("tier_value_int", [0.25, 0.5, 0.75]).alias("tier_value_int_quartiles"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sum(F.when(F.col("plan") == "basic", F.col("plan_max_tier_value_int")).otherwise(0)).alias("basic_plan_max_tier_value_int"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.percentile_approx("tier_value", 0.9).alias("tier_value_p90"),
    )
)
