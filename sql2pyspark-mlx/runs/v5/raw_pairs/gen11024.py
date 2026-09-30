from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("currency_min_element").asc_nulls_last())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

matched_keys = (
    flattened
    .select("plan")
    .subtract(
        df_price_tiers
        .filter(F.col("effective_from") >= "2025-02-11")
        .select("plan")
    )
)

reconciled = (
    flattened
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("effective_from").between("2023-05-01", "2023-08-06"))
    .select(
        "id",
        "effective_from",
        "currency",
        "element",
        F.date_trunc("year", "effective_from").alias("effective_from_period"),
        F.concat_ws("/", "currency", "plan").alias("currency_plan_key"),
    )
)

scored = (
    valid
    .filter(F.col("effective_from") > "2023-08-01")
    .select(
        "effective_from",
        "currency_plan_key",
        F.min("element").over(w_hist).alias("currency_min_element"),
    )
)

metrics = (
    scored
    .select(
        "effective_from",
        "currency_plan_key",
        F.cume_dist().over(w2).alias("currency_min_element_cume_dist"),
        F.percent_rank().over(w2).alias("currency_min_element_percent_rank"),
    )
)

result = (
    metrics
    .select("effective_from", "currency_plan_key", "currency_min_element_cume_dist")
    .orderBy(F.col("effective_from").desc_nulls_first())
)
