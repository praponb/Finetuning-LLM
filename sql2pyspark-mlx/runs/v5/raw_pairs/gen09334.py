from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc_nulls_last())
w2 = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy("element")
w4 = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_first())
w5 = Window.orderBy(F.col("element_cume_dist").asc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("element"))
)

base = (
    exploded
    .filter(F.col("effective_from") > F.lit("2024-02-08").cast("date"))
    .select(
        "*",
        F.initcap(F.trim("currency")).alias("currency_title"),
        F.date_add("effective_from", 20).alias("effective_from_plus_20d"),
    )
)

enriched = (
    base
    .select(
        "effective_from",
        "currency",
        "element",
        "currency_title",
        "effective_from_plus_20d",
        F.cume_dist().over(w).alias("element_cume_dist"),
        F.stddev_samp("element").over(w2).alias("currency_stddev_samp_element"),
        F.percent_rank().over(w3).alias("element_percent_rank"),
        F.count("element").over(w2).alias("currency_count_element"),
        F.dense_rank().over(w4).alias("element_dense_rank"),
    )
)

matched_keys = (
    enriched
    .select("currency")
    .intersectAll(
        df_price_tiers
        .filter(F.col("plan").isin(["enterprise", "starter"]))
        .select("currency")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "effective_from",
        "currency",
        "currency_title",
        "effective_from_plus_20d",
        "element_cume_dist",
        "element_percent_rank",
        "currency_count_element",
        F.rank().over(w5).alias("element_cume_dist_rank"),
    )
)

cleaned = (
    scored
    .filter(F.col("effective_from") > F.lit("2024-05-23").cast("date"))
    .withColumn("element_percent_rank_plus_currency_count_element", (F.col("element_percent_rank") + F.col("currency_count_element")))
)

result = (
    cleaned
    .select(
        "effective_from",
        "currency",
        "currency_title",
        "effective_from_plus_20d",
        "element_cume_dist",
        "element_percent_rank",
        "currency_count_element",
    )
)
