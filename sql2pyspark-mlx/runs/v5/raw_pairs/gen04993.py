from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("element"),
    )
)

scored = (
    exploded
    .filter(F.col("plan").isin(["starter", "basic"]))
    .withColumn("currency_min_element", F.min("element").over(w))
    .withColumn("element_rank", F.rank().over(w2))
)

base = (
    scored
    .filter(F.col("element") > 56)
    .withColumn("element_rank_per_element", (F.col("element_rank") / F.nullif("element", F.lit(0))))
    .withColumn("plan_prefix", F.substring("plan", 1, 3))
)

cleaned = (
    base
    .filter(F.col("element").between(59, 75))
    .select("*", F.date_trunc("year", "effective_from").alias("effective_from_period"))
)

valid = (
    cleaned
    .filter(F.col("currency") == "EUR")
    .withColumn("currency_lower", F.lower("currency"))
    .withColumn("effective_from_period_label", F.date_format("effective_from_period", "MMM yyyy"))
)

valid_price_tiers = (
    valid
    .filter(F.col("effective_from").between("2024-04-11", "2025-06-02"))
    .withColumn("element_rank_per_element_plus_currency_min_element", (F.col("element_rank_per_element") + F.col("currency_min_element")))
)

base_price_tiers = (
    valid_price_tiers
    .filter(F.col("effective_from") >= F.lit("2024-05-11").cast("date"))
    .withColumn("plan_currency", F.concat("plan", F.lit("/"), "currency"))
)

result = (
    base_price_tiers
    .select(
        "id",
        "effective_from",
        "element",
        "plan_prefix",
        "currency_lower",
        "plan_currency",
    )
    .orderBy(F.col("effective_from").desc_nulls_last())
)
