from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("element"))
)

valid = (
    flattened
    .filter(F.col("currency") == "USD")
    .withColumn("plan_length", F.length("plan"))
)

base = (
    valid
    .filter(F.col("plan_length") == 72)
    .select(
        "*",
        F.when(F.col("currency").isin(["USD", "THB"]), "Priority").otherwise("Other").alias("currency_group"),
    )
)

base_price_tiers = (
    base
    .filter(F.col("currency").isin(["THB", "USD", "EUR"]))
    .select(
        "id",
        "currency",
        "element",
        "plan_length",
        "currency_group",
        (F.col("element") * F.col("plan_length")).alias("element_times_plan_length"),
    )
)

stats = (
    base_price_tiers
    .rollup(
        "currency",
        F.when(F.col("element") >= 74, "LARGE").otherwise("SMALL").alias("element_tier"),
    )
    .agg(
        F.max(F.when(F.col("currency") == "USD", F.col("plan_length"))).alias("usd_max_plan_length"),
        F.percentile_approx("element", 0.95).alias("element_p95"),
        F.countDistinct("currency").alias("distinct_currencies"),
    )
)

result = (
    stats
    .select("element_tier", "usd_max_plan_length", "element_p95")
    .orderBy(F.col("usd_max_plan_length").asc_nulls_first(), "element_tier")
)
