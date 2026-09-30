from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("days_since_effective_from").desc_nulls_last())

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_prices").alias("element"))
)

cleaned = (
    flattened
    .filter((F.col("effective_from") >= F.lit("2024-03-15").cast("date")) & (F.col("effective_from") <= "2025-05-01"))
    .withColumn("days_since_effective_from", F.datediff(F.current_date(), "effective_from"))
    .withColumn("effective_from_month_end", F.last_day("effective_from"))
)

with_windows = (
    cleaned
    .filter(F.col("element") < 52)
    .withColumn("plan_sum_element", F.sum("element").over(w))
    .withColumn("days_since_effective_from_rank", F.rank().over(w2))
    .withColumn("plan_min_element", F.min("element").over(w))
    .withColumn("plan_min_days_since_effective_from", F.min("days_since_effective_from").over(w))
)

cleaned_price_tiers = (
    with_windows
    .filter(F.col("element") <= 69)
    .select(
        "id",
        "plan",
        "effective_from",
        "element",
        "effective_from_month_end",
        "plan_sum_element",
        "plan_min_element",
        "plan_min_days_since_effective_from",
        F.greatest("element", "plan_min_element").alias("greatest_element_plan_min_element"),
    )
)

result = (
    cleaned_price_tiers
    .groupBy(F.date_trunc("WEEK", "effective_from_month_end").alias("week_start"))
    .agg(
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.sum(F.when(F.col("plan") == "enterprise", F.col("plan_sum_element")).otherwise(0)).alias("enterprise_plan_sum_element"),
        F.round(F.avg("greatest_element_plan_min_element"), 2).alias("avg_greatest_element_plan_min_element"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.avg(F.when(F.col("plan") == "basic", F.col("greatest_element_plan_min_element"))).alias("basic_avg_greatest_element_plan_min_element"),
    )
    .filter(F.col("basic_avg_greatest_element_plan_min_element") > 0)
)
