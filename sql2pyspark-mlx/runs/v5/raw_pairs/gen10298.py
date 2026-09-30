from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_last())
w_part = Window.partitionBy("currency")
w3 = Window.orderBy(F.col("element_plus_element_rounded").asc_nulls_last())
w4 = Window.orderBy(F.col("element_plus_element_rounded").desc_nulls_first())
w5 = Window.orderBy(F.col("element_plus_element_rounded_rank").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

filtered = (
    lines
    .filter(F.col("element") > 64)
    .withColumn("currency_upper", F.upper("currency"))
)

base = (
    filtered
    .filter(F.col("element") <= 49)
    .select(
        "currency",
        "element",
        "currency_upper",
        F.substring("currency", 1, 2).alias("currency_prefix"),
        F.date_format("effective_from", "yyyy-MM-dd").alias("effective_from_label"),
    )
)

filtered_price_tiers = (
    base
    .filter(F.col("element") >= 20)
    .withColumn("element_rounded", F.round("element", 1))
    .withColumn("element_tier", F.when(F.col("element") >= 60, "High").when(F.col("element") >= 31, "Medium").otherwise("Low"))
)

filtered_price_tiers_2 = (
    filtered_price_tiers
    .filter(F.col("currency").isin(["EUR", "THB"]))
    .withColumn("element_rounded_whole", F.floor("element_rounded"))
    .withColumn("element_plus_element_rounded", (F.col("element") + F.col("element_rounded")))
)

metrics = (
    filtered_price_tiers_2
    .select(
        "currency",
        "element",
        "currency_upper",
        "effective_from_label",
        "element_rounded",
        "element_plus_element_rounded",
        F.rank().over(w).alias("element_rank"),
        F.min("element_plus_element_rounded").over(w_part).alias("currency_min_element_plus_element_rounded"),
        F.rank().over(w3).alias("element_plus_element_rounded_rank"),
    )
)

enriched = (
    metrics
    .filter(F.col("element_rounded") < 58)
    .withColumn("element_plus_element_rounded_cume_dist", F.cume_dist().over(w4))
    .withColumn("currency_min_element_rank", F.min("element_rank").over(w_part))
    .withColumn("element_plus_element_rounded_rank_dense_rank", F.dense_rank().over(w5))
    .withColumn("element_rank_share", F.round((F.col("element_rank") / F.sum("element_rank").over(w_part)), 4))
)

result = (
    enriched
    .select(
        "currency",
        "element",
        "currency_upper",
        "effective_from_label",
        "element_rank",
        "element_plus_element_rounded_rank",
        "element_plus_element_rounded_rank_dense_rank",
        "element_rank_share",
    )
    .orderBy(F.col("element").desc_nulls_first(), "currency")
)
