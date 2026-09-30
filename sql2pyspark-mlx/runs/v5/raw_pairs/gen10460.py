from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("element"))
)

valid = (
    lines
    .filter(F.col("element") <= 34)
    .select("*", F.length("currency").alias("currency_length"))
)

filtered = (
    valid
    .filter(F.col("currency").isin(["THB", "EUR", "USD"]))
    .select(
        "id",
        "plan",
        "currency",
        "element",
        "currency_length",
        F.concat_ws("/", "currency", "plan").alias("currency_plan_key"),
        F.lower("plan").alias("plan_lower"),
    )
)

cleaned = (
    filtered
    .filter(F.col("element").between(30, 48))
    .select(
        "plan",
        "element",
        "currency_length",
        "plan_lower",
        F.concat("plan", F.lit(", "), "currency").alias("plan_currency"),
        (F.col("currency_length") + F.col("element")).alias("currency_length_plus_element"),
    )
)

result = (
    cleaned
    .rollup("plan")
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.countDistinct("plan").alias("distinct_plans_2"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("distinct_plans") < 1000)
)
