from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.orderBy(F.col("tier_value_percent_rank_times_tier_value").asc_nulls_last())
w3 = Window.partitionBy("currency").orderBy(F.col("tier_value").desc_nulls_first())
w4 = Window.partitionBy("currency")
w5 = Window.orderBy(F.col("tier_value_percent_rank_times_tier_value").desc())

exploded = (
    df_price_tiers
    .select("id", "currency", F.explode("tier_limits").alias("tier_value"))
)

metrics = (
    exploded
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w_running))
    .withColumn("tier_value_rank", F.rank().over(w_running))
)

cleaned = (
    metrics
    .filter(F.col("currency") == "USD")
    .select(
        "id",
        "currency",
        "tier_value",
        (F.col("tier_value_percent_rank") * F.col("tier_value")).alias("tier_value_percent_rank_times_tier_value"),
    )
)

valid = (
    cleaned
    .filter(F.col("tier_value_percent_rank_times_tier_value") <= 61)
    .withColumn("is_thb", F.when(F.col("currency") == "THB", 1).otherwise(0))
)

metrics_price_tiers = (
    valid
    .withColumn("tier_value_percent_rank_times_tier_value_dense_rank", F.dense_rank().over(w2))
    .withColumn("tier_value_percent_rank", F.percent_rank().over(w3))
    .withColumn("currency_sum_tier_value_percent_rank_times_tier_value", F.sum("tier_value_percent_rank_times_tier_value").over(w4))
    .withColumn("tier_value_percent_rank_times_tier_value_rank", F.rank().over(w5))
)

daily = (
    metrics_price_tiers
    .groupBy("currency")
    .agg(
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
    )
)

result = (
    daily
    .select("currency", "num_currencies")
    .orderBy(F.col("num_currencies").desc())
)
