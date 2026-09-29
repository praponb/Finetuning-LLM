from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy(F.col("tier_value").asc_nulls_last())
w2 = Window.partitionBy("plan")
w_recent = Window.partitionBy("plan").orderBy("plan_length")
w4 = Window.orderBy("tier_value")
w5 = Window.orderBy("plan_length_dense_rank")
w6 = Window.orderBy(F.col("tier_value").asc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

filtered = (
    exploded
    .filter(F.col("tier_value") <= 58)
    .withColumn("is_basic", F.when(F.col("plan") == "basic", 1).otherwise(0))
    .withColumn("plan_prefix", F.substring("plan", 1, 2))
)

base = (
    filtered
    .filter((F.col("effective_from") >= F.lit("2024-06-04").cast("date")) & (F.col("is_basic") > 77))
    .withColumn("greatest_is_basic_tier_value", F.greatest("is_basic", "tier_value"))
)

filtered_price_tiers = (
    base
    .filter(F.col("plan") == "pro")
    .select(
        "id",
        "plan",
        "effective_from",
        "tier_value",
        "plan_prefix",
        "greatest_is_basic_tier_value",
        F.length("plan").alias("plan_length"),
        F.least("is_basic", "greatest_is_basic_tier_value").alias("least_is_basic_greatest_is_basic_tier_value"),
    )
)

scored = (
    filtered_price_tiers
    .filter(F.col("plan") == "basic")
    .select(
        "id",
        "effective_from",
        "tier_value",
        "plan_prefix",
        "greatest_is_basic_tier_value",
        "plan_length",
        F.rank().over(w_ordered).alias("tier_value_rank"),
        F.max("tier_value").over(w2).alias("plan_max_tier_value"),
        F.dense_rank().over(w_recent).alias("plan_length_dense_rank"),
    )
)

valid = (
    scored
    .filter(F.col("effective_from") >= F.to_date(F.lit("2024-04-15")))
    .select(
        "*",
        (F.col("tier_value") * F.col("greatest_is_basic_tier_value")).alias("tier_value_times_greatest_is_basic_tier_value"),
    )
)

result = (
    valid
    .select(
        "tier_value",
        "plan_length",
        "plan_max_tier_value",
        "tier_value_times_greatest_is_basic_tier_value",
        F.cume_dist().over(w4).alias("tier_value_cume_dist"),
        F.dense_rank().over(w5).alias("plan_length_dense_rank_dense_rank"),
        F.cume_dist().over(w6).alias("tier_value_cume_dist_2"),
    )
)
