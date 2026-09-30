from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan").orderBy(F.col("element").desc())
w3 = Window.partitionBy("plan", "currency").orderBy(F.col("element").asc_nulls_last())
w4 = Window.partitionBy("plan").orderBy("element")

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

filtered = (
    exploded
    .filter((F.col("effective_from") >= F.to_date(F.lit("2024-02-07"))) & F.col("element").between(30, 47))
    .select("*", F.date_sub("effective_from", 77).alias("effective_from_minus_77d"))
)

base = (
    filtered
    .filter((F.col("effective_from") >= F.to_date(F.lit("2024-06-15"))) & (F.col("element") == 25))
    .select(
        "*",
        F.date_format("effective_from", "yyyy-MM").alias("effective_from_label"),
    )
)

metrics = (
    base
    .filter(F.col("effective_from").between("2024-02-09", "2024-06-01"))
    .select(
        "id",
        "plan",
        "currency",
        "element",
        "effective_from_minus_77d",
        "effective_from_label",
        F.min("element").over(w).alias("plan_min_element"),
        F.percent_rank().over(w2).alias("element_percent_rank"),
        F.rank().over(w3).alias("element_rank"),
        F.rank().over(w4).alias("element_rank_2"),
    )
)

filtered_price_tiers = (
    metrics
    .filter(F.col("element_rank_2") != 4)
    .select(
        "effective_from_minus_77d",
        "effective_from_label",
        "plan_min_element",
        "element_rank",
        "element_rank_2",
        F.substring("plan", 1, 3).alias("plan_prefix"),
    )
)

summary = (
    filtered_price_tiers
    .groupBy(
        F.when(F.col("element_rank") >= 7, "LARGE").otherwise("SMALL").alias("element_rank_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("element_rank_2", 0.5).alias("element_rank_2_p50"),
        F.percentile_approx("element_rank_2", 0.95).alias("element_rank_2_p95"),
        F.max("plan_min_element").alias("max_plan_min_element"),
        F.percentile_approx("element_rank_2", [0.25, 0.5, 0.75]).alias("element_rank_2_quartiles"),
        F.sum("element_rank").alias("total_element_rank"),
    )
)

result = (
    summary
    .select(
        "element_rank_tier",
        "row_count",
        "element_rank_2_p50",
        "element_rank_2_p95",
        "max_plan_min_element",
        "total_element_rank",
    )
)
