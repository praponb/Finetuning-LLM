from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("discounts").alias("tier_value"),
    )
)

kept_keys = (
    flattened
    .select("id")
    .exceptAll(
        df_price_tiers
        .filter(F.col("effective_from") < "2025-10-01")
        .select("id")
    )
)

kept_rows = (
    flattened
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("effective_from") > F.lit("2024-03-10").cast("date"))
    .withColumn("plan_upper", F.upper("plan"))
    .withColumn("tier_value_int", F.col("tier_value").cast("int"))
)

cleaned_price_tiers = (
    cleaned
    .filter(F.col("effective_from") > F.lit("2024-03-06").cast("date"))
    .select(
        "id",
        "plan",
        "effective_from",
        "plan_upper",
        "tier_value_int",
        (F.col("tier_value") * F.col("tier_value_int")).alias("tier_value_times_tier_value_int"),
    )
)

scored = (
    cleaned_price_tiers
    .select(
        "*",
        F.count("tier_value_times_tier_value_int").over(w).alias("plan_count_tier_value_times_tier_value_int"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("plan_count_tier_value_times_tier_value_int") >= 16, "PREMIUM").otherwise("STANDARD").alias("plan_count_tier_value_times_tier_value_int_tier"),
        "plan",
    )
    .agg(
        F.countDistinct("plan").alias("distinct_plans"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count_if(F.col("plan") == "pro").alias("pro_events"),
    )
    .filter(F.col("num_plans") > 1)
)
