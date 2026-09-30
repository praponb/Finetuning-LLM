from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("element").desc_nulls_last())
w3 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("element_percent_rank").asc_nulls_first())
w5 = Window.orderBy(F.col("element_percent_rank").desc())
w6 = Window.partitionBy("plan").orderBy(F.col("element_percent_rank").asc_nulls_last())
w7 = Window.orderBy(F.col("currency_min_element").desc_nulls_first())
w8 = Window.partitionBy("plan").orderBy(F.col("avg_element_cume_dist").desc())

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

enriched = (
    flattened
    .filter(F.col("effective_from") <= "2023-08-01")
    .select(
        "id",
        "plan",
        F.min("element").over(w).alias("currency_min_element"),
        F.cume_dist().over(w2).alias("element_cume_dist"),
        F.percent_rank().over(w3).alias("element_percent_rank"),
    )
)

scored = (
    enriched
    .select("*", F.dense_rank().over(w4).alias("element_percent_rank_dense_rank"))
)

scored_price_tiers = (
    scored
    .select(
        "*",
        F.rank().over(w5).alias("element_percent_rank_rank"),
        F.percent_rank().over(w6).alias("element_percent_rank_percent_rank"),
        F.cume_dist().over(w7).alias("currency_min_element_cume_dist"),
    )
)

valid = (
    scored_price_tiers
    .filter(F.col("element_cume_dist") <= 0.39)
    .withColumn("element_percent_rank_rank_whole", F.ceil("element_percent_rank_rank"))
)

base = (
    valid
    .filter(F.col("element_percent_rank") >= 0.67)
    .select(
        "id",
        "plan",
        "currency_min_element",
        "element_cume_dist",
        "element_percent_rank",
        "element_percent_rank_rank",
        "currency_min_element_cume_dist",
        "element_percent_rank_rank_whole",
        (F.col("element_percent_rank_dense_rank") - F.col("element_percent_rank_rank_whole")).alias("element_percent_rank_dense_rank_minus_element_percent_rank_rank_whole"),
    )
)

cleaned = (
    base
    .filter(F.col("element_percent_rank_dense_rank_minus_element_percent_rank_rank_whole") >= 29)
    .select(
        "*",
        F.when(F.col("element_percent_rank_dense_rank_minus_element_percent_rank_rank_whole") >= 62, "High").when(F.col("element_percent_rank_dense_rank_minus_element_percent_rank_rank_whole") >= 12, "Medium").otherwise("Low").alias("element_percent_rank_dense_rank_minus_element_percent_rank_rank_whole_level"),
    )
)

result = (
    cleaned
    .groupBy(
        "plan",
        F.when(F.col("element_percent_rank") >= 0.63, "PREMIUM").otherwise("STANDARD").alias("element_percent_rank_tier"),
    )
    .agg(
        F.percentile_approx("element_cume_dist", 0.5).alias("element_cume_dist_p50"),
        F.round(F.avg("element_cume_dist"), 2).alias("avg_element_cume_dist"),
        F.countDistinct("plan").alias("distinct_plans"),
    )
    .filter(F.col("distinct_plans") < 1000)
    .withColumn("avg_element_cume_dist_rank", F.rank().over(w8))
)
