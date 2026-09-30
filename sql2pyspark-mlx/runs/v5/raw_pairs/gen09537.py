from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc_nulls_last())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("tier_prices").alias("element"))
)

with_windows = (
    lines
    .select("id", "element", F.percent_rank().over(w).alias("element_percent_rank"))
)

base = (
    with_windows
    .filter(F.col("element") < 40)
    .select("*", F.ceil("element_percent_rank").alias("element_percent_rank_whole"))
)

remaining_keys = (
    base
    .select("id")
    .subtract(
        df_price_tiers
        .filter(F.col("plan") == "pro")
        .select("id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base_price_tiers = (
    kept_rows
    .filter(F.col("element_percent_rank") <= 0.61)
    .select(
        "*",
        (F.col("element_percent_rank") + F.col("element_percent_rank_whole")).alias("element_percent_rank_plus_element_percent_rank_whole"),
    )
)

result = (
    base_price_tiers
    .groupBy(
        F.when(F.col("element") >= 67, "HIGH").otherwise("LOW").alias("element_tier"),
        F.when(F.col("element_percent_rank") >= 0.76, "PREMIUM").otherwise("STANDARD").alias("element_percent_rank_tier"),
        F.when(F.col("element_percent_rank_plus_element_percent_rank_whole") >= 87, "LARGE").otherwise("SMALL").alias("element_percent_rank_plus_element_percent_rank_whole_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)
