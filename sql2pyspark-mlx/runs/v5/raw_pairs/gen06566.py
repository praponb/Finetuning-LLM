from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").desc_nulls_last())
w2 = Window.orderBy(F.col("plan_length").desc())
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("plan").orderBy(F.col("plan_length").desc())

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("tier_value"))
)

cleaned = (
    lines
    .filter(F.col("currency") == "USD")
    .select("*", F.initcap(F.trim("plan")).alias("plan_title"))
)

base = (
    cleaned
    .filter(F.col("tier_value") <= 76)
    .select(
        "id",
        "plan",
        "tier_value",
        "plan_title",
        F.length("plan").alias("plan_length"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.percent_rank().over(w).alias("tier_value_percent_rank"),
        F.rank().over(w2).alias("plan_length_rank"),
        F.count("tier_value").over(w3).alias("plan_count_tier_value"),
        F.percent_rank().over(w4).alias("plan_length_percent_rank"),
        F.count("tier_value").over(w3).alias("plan_count_tier_value_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("plan_length") >= 73, "LARGE").otherwise("SMALL").alias("plan_length_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.count_if(F.col("plan") == "pro").alias("pro_events"),
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
    )
)
