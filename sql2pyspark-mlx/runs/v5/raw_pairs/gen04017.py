from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w_running = Window.partitionBy("plan")

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_prices").alias("element"))
)

enriched = (
    lines
    .filter(F.col("element") < 36)
    .select("*", F.max("element").over(w).alias("currency_max_element"))
)

cleaned = (
    enriched
    .filter(F.col("currency").isin(["USD", "THB", "EUR"]))
    .select(
        "id",
        "plan",
        "currency",
        "element",
        "currency_max_element",
        F.upper("plan").alias("plan_upper"),
    )
)

with_windows = (
    cleaned
    .select(
        "currency",
        "element",
        "currency_max_element",
        F.count("element").over(w_running).alias("plan_count_element"),
        F.sum("element").over(w_running).alias("plan_sum_element"),
    )
)

valid = (
    with_windows
    .filter(F.col("currency") == "EUR")
    .select(
        "element",
        "plan_count_element",
        "plan_sum_element",
        F.round("plan_sum_element", 2).alias("plan_sum_element_rounded"),
    )
)

result = (
    valid
    .select(
        "element",
        "plan_count_element",
        "plan_sum_element",
        "plan_sum_element_rounded",
    )
)
