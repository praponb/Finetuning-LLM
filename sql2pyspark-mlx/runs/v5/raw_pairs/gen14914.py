from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("element_str")

lines = df_price_tiers.select("id", F.explode("tier_limits").alias("element"))

cleaned = (
    lines
    .filter(F.col("element").between(46, 54))
    .select("*", F.col("element").cast("double").alias("element_str"))
)

valid = (
    cleaned
    .filter(F.col("element").isNull())
    .withColumn("element_rounded", F.round("element", 1))
)

enriched = valid.select("id", "element", F.rank().over(w).alias("element_str_rank"))

grouped = (
    enriched
    .groupBy(
        F.when(F.col("element_str_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("element_str_rank_tier"),
    )
    .agg(
        F.round(F.avg("element_str_rank"), 2).alias("avg_element_str_rank"),
        F.percentile_approx("element_str_rank", 0.95).alias("element_str_rank_p95"),
        F.count("*").alias("cnt"),
        F.percentile_approx("element_str_rank", [0.25, 0.5, 0.75]).alias("element_str_rank_quartiles"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.count("*").alias("event_count"),
    )
)

result = (
    grouped
    .select(
        "element_str_rank_tier",
        "avg_element_str_rank",
        "element_str_rank_quartiles",
        "element_quartiles",
        "event_count",
    )
    .orderBy(F.col("avg_element_str_rank").asc_nulls_last())
)
