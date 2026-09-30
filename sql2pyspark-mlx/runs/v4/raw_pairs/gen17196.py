from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("tier_value").asc_nulls_last())
w_running = Window.partitionBy("currency")
w3 = Window.orderBy(F.col("tier_value").desc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

filtered = (
    lines
    .filter(F.col("effective_from") > F.lit("2024-09-20").cast("date"))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_value",
        F.upper("currency").alias("currency_upper"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.rank().over(w).alias("tier_value_rank"),
        F.rank().over(w).alias("tier_value_rank_2"),
    )
)

enriched_price_tiers = (
    enriched
    .withColumn("currency_max_tier_value", F.max("tier_value").over(w_running))
    .withColumn("tier_value_dense_rank", F.dense_rank().over(w3))
)

filtered_price_tiers = (
    enriched_price_tiers
    .filter(F.col("currency") == "THB")
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "tier_value",
        "currency_upper",
        "tier_value_rank",
        "tier_value_rank_2",
        "currency_max_tier_value",
        "tier_value_dense_rank",
        (F.col("tier_value") * F.col("currency_max_tier_value")).alias("tier_value_times_currency_max_tier_value"),
    )
)

valid = (
    filtered_price_tiers
    .filter(F.col("tier_value_times_currency_max_tier_value") <= 39)
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_value_rank",
        "tier_value_rank_2",
        "tier_value_dense_rank",
        "tier_value_times_currency_max_tier_value",
        (F.col("tier_value_dense_rank") - F.col("tier_value_times_currency_max_tier_value")).alias("tier_value_dense_rank_minus_tier_value_times_currency_max_tier_value"),
    )
)

report = (
    valid
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_value_rank",
        "tier_value_rank_2",
        "tier_value_dense_rank",
        "tier_value_times_currency_max_tier_value",
        F.when(F.col("plan").rlike("^(basic|enterprise)$"), "GROUP_B").when(F.col("plan").rlike("^(starter|pro)$"), "GROUP_A").otherwise("EXTENDED").alias("plan_zone"),
    )
)

result = (
    report
    .select(
        "effective_from",
        "tier_value_rank_2",
        "tier_value_times_currency_max_tier_value",
        "plan_zone",
        F.when(F.col("plan_zone").rlike("^(GROUP_A|GROUP_B)$"), "GROUP_B").when(F.col("plan_zone").rlike("^(EXTENDED)$"), "GROUP_A").otherwise("EXTENDED").alias("plan_zone_zone"),
    )
    .orderBy("plan_zone_zone", F.col("effective_from").desc())
)
