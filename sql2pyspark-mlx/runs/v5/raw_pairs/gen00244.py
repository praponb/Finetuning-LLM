from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value_str").desc())
w2 = Window.partitionBy("plan")
w3 = Window.orderBy(F.col("tier_value_str").desc_nulls_first())
w4 = Window.partitionBy("plan").orderBy(F.col("plan_sum_plan_avg_tier_value_str").asc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_limits").alias("tier_value"),
    )
)

filtered = (
    exploded
    .filter(F.col("effective_from") > F.lit("2024-03-06").cast("date"))
    .select("*", F.col("tier_value").cast("double").alias("tier_value_str"))
)

metrics = (
    filtered
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_value",
        "tier_value_str",
        F.dense_rank().over(w).alias("tier_value_str_dense_rank"),
        F.avg("tier_value_str").over(w2).alias("plan_avg_tier_value_str"),
        F.dense_rank().over(w3).alias("tier_value_str_dense_rank_2"),
    )
)

with_windows = (
    metrics
    .withColumn("plan_sum_plan_avg_tier_value_str", F.sum("plan_avg_tier_value_str").over(w2))
)

metrics_price_tiers = (
    with_windows
    .filter(F.col("plan_avg_tier_value_str") < 26)
    .select(
        "*",
        F.max("tier_value").over(w2).alias("plan_max_tier_value"),
        F.sum("plan_sum_plan_avg_tier_value_str").over(w2).alias("plan_sum_plan_sum_plan_avg_tier_value_str"),
        F.percent_rank().over(w4).alias("plan_sum_plan_avg_tier_value_str_percent_rank"),
    )
)

remaining_keys = (
    metrics_price_tiers
    .select("id")
    .intersect(
        df_price_tiers
        .filter(F.col("currency") != "EUR")
        .select("id")
    )
)

verified = (
    metrics_price_tiers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "plan",
        F.col("plan_sum_plan_avg_tier_value_str_percent_rank").alias("PLAN_SUM_PLAN_AVG_TIER_VALUE_STR_PERCENT_RANK_VAL"),
        F.col("tier_value_str").alias("TIER_VALUE_STR_VAL"),
    )
    .unpivot(
        ids=["plan"],
        values=["PLAN_SUM_PLAN_AVG_TIER_VALUE_STR_PERCENT_RANK_VAL", "TIER_VALUE_STR_VAL"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
