from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc())
w2 = Window.orderBy(F.col("element").asc_nulls_first())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

enriched = (
    flattened
    .select(
        "id",
        "plan",
        "currency",
        "element",
        F.rank().over(w).alias("element_rank"),
        F.percent_rank().over(w2).alias("element_percent_rank"),
    )
)

valid = (
    enriched
    .filter(F.col("element") == 24)
    .select(
        "id",
        "currency",
        "element",
        F.concat("currency", F.lit(" "), "plan").alias("currency_plan"),
        F.coalesce("element_percent_rank", F.lit(0)).alias("element_percent_rank_filled"),
    )
)

filtered = (
    valid
    .filter(F.col("currency").isin(["USD", "THB", "EUR"]))
    .select(
        "id",
        "currency",
        "element",
        "currency_plan",
        "element_percent_rank_filled",
        (F.col("element_percent_rank_filled") * F.col("element")).alias("element_percent_rank_filled_times_element"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("element_percent_rank_filled") >= 86, "HIGH").otherwise("LOW").alias("element_percent_rank_filled_tier"),
    )
    .agg(
        F.avg(F.when(F.col("currency") == "USD", F.col("element_percent_rank_filled_times_element"))).alias("usd_avg_element_percent_rank_filled_times_element"),
        F.max("element").alias("max_element"),
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.percentile_approx("element_percent_rank_filled_times_element", 0.95).alias("element_percent_rank_filled_times_element_p95"),
    )
    .filter(F.col("usd_avg_element_percent_rank_filled_times_element") > 0)
)
