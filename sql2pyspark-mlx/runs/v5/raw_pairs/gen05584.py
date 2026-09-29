from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("tier_value").desc())

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("tier_value"))
)

metrics = flattened.select("*", F.rank().over(w).alias("tier_value_rank"))

base = (
    metrics
    .filter(F.col("plan") == "starter")
    .select("*", F.concat_ws(",", "currency", "plan").alias("currency_plan_key"))
)

valid = (
    base
    .filter(F.col("plan") != "pro")
    .withColumn("tier_value_rank_filled", F.coalesce("tier_value_rank", F.lit(0)))
    .withColumn("tier_value_minus_tier_value_rank", (F.col("tier_value") - F.col("tier_value_rank")))
)

base_price_tiers = (
    valid
    .filter(F.col("tier_value_rank") < 5)
    .select(
        "id",
        "plan",
        "currency",
        "tier_value",
        "tier_value_rank",
        "currency_plan_key",
        "tier_value_rank_filled",
        F.length("currency").alias("currency_length"),
        (F.col("tier_value") * F.col("tier_value_rank")).alias("tier_value_times_tier_value_rank"),
    )
)

cleaned = (
    base_price_tiers
    .filter(F.col("tier_value_rank").between(4, 6))
    .select(
        "id",
        "plan",
        "currency",
        "tier_value_rank",
        "currency_plan_key",
        "tier_value_rank_filled",
        "currency_length",
        "tier_value_times_tier_value_rank",
        F.concat("currency", F.lit(" "), "plan").alias("currency_plan"),
        F.round("tier_value_rank", 1).alias("tier_value_rank_rounded"),
    )
)

result = (
    cleaned
    .select(
        "currency",
        "tier_value_times_tier_value_rank",
        F.col("tier_value_rank_rounded").alias("TIER_VALUE_RANK_ROUNDED"),
        F.col("tier_value_rank_filled").alias("TIER_VALUE_RANK_FILLED"),
    )
    .unpivot(
        ids=["currency"],
        values=["tier_value_times_tier_value_rank", "TIER_VALUE_RANK_ROUNDED", "TIER_VALUE_RANK_FILLED"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
)
