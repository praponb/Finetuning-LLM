from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())
w2 = Window.orderBy(F.col("element").desc_nulls_first())
w3 = Window.partitionBy("plan").orderBy(F.col("element").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("element"),
    )
)

valid = (
    flattened
    .filter((F.col("effective_from") >= F.lit("2024-07-22").cast("date")) & (F.col("currency") == "USD"))
    .select(
        "id",
        "plan",
        "effective_from",
        "element",
        F.concat("plan", F.lit(" - "), "currency").alias("plan_currency"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.percent_rank().over(w).alias("element_percent_rank"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
        F.rank().over(w3).alias("element_rank"),
    )
)

base = (
    scored
    .filter(F.col("element").between(38, 61))
    .withColumn("plan_lower", F.lower("plan"))
)

valid_price_tiers = (
    base
    .filter((F.col("effective_from") >= F.lit("2024-05-29").cast("date")) & (F.col("element_percent_rank") >= 0.68))
    .select(
        "*",
        F.coalesce("element_rank", F.lit(0)).alias("element_rank_filled"),
        F.round((F.col("element_percent_rank") * 0.9), 2).alias("element_percent_rank_adjusted"),
    )
)

cleaned = (
    valid_price_tiers
    .filter(F.col("element_dense_rank").between(4, 7))
    .withColumn("element_percent_rank_adjusted_scaled", F.round((F.col("element_percent_rank_adjusted") * 100), 2))
)

cleaned_price_tiers = (
    cleaned
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-07-07"))) & F.col("plan").isin(["enterprise", "pro"]))
    .select(
        "plan",
        "effective_from",
        "element_percent_rank",
        "element_dense_rank",
        "element_rank",
        "element_rank_filled",
        "element_percent_rank_adjusted",
        "element_percent_rank_adjusted_scaled",
        (F.col("element_dense_rank") - F.col("element_percent_rank_adjusted_scaled")).alias("element_dense_rank_minus_element_percent_rank_adjusted_scaled"),
    )
)

result = (
    cleaned_price_tiers
    .select("plan", "element_percent_rank_adjusted", "element_percent_rank")
    .unpivot(
        ["plan"],
        ["element_percent_rank_adjusted", "element_percent_rank"],
        "metric",
        "value",
    )
)
