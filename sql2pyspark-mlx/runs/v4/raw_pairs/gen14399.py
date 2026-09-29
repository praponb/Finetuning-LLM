from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_last())
w2 = Window.partitionBy("currency").orderBy(F.col("element").asc_nulls_first())
w3 = Window.orderBy(F.col("element").desc())
w_hist_2 = Window.partitionBy("currency").orderBy(F.col("element").desc())
w5 = Window.partitionBy("currency").orderBy(F.col("element_cume_dist").desc())
w6 = Window.partitionBy("currency").orderBy(F.col("element_percent_rank_2").desc())
w7 = Window.partitionBy("currency").orderBy("element")
w8 = Window.partitionBy("currency").orderBy("element_percent_rank_2")
w9 = Window.partitionBy("currency").orderBy("element_percent_rank_2_per_element_rank_per_element_percent_rank_3")
w_running = Window.orderBy(F.col("element_percent_rank").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

metrics = (
    exploded
    .select(
        "*",
        F.rank().over(w_hist).alias("element_rank"),
        F.percent_rank().over(w2).alias("element_percent_rank"),
        F.cume_dist().over(w3).alias("element_cume_dist"),
        F.rank().over(w_hist).alias("element_rank_2"),
        F.percent_rank().over(w_hist_2).alias("element_percent_rank_2"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.cume_dist().over(w5).alias("element_cume_dist_cume_dist"),
        F.percent_rank().over(w6).alias("element_percent_rank_2_percent_rank"),
        F.percent_rank().over(w7).alias("element_percent_rank_3"),
    )
)

filtered = (
    enriched
    .filter(F.col("element_cume_dist") <= 0.8)
    .select(
        "id",
        "effective_from",
        "currency",
        "element",
        "element_rank",
        "element_percent_rank",
        "element_rank_2",
        "element_percent_rank_2",
        "element_cume_dist_cume_dist",
        "element_percent_rank_2_percent_rank",
        "element_percent_rank_3",
        (F.col("element_rank") / F.nullif("element_percent_rank_3", F.lit(0))).alias("element_rank_per_element_percent_rank_3"),
    )
)

valid = (
    filtered
    .filter(F.col("effective_from") > F.lit("2024-10-29").cast("date"))
    .select(
        "*",
        (F.col("element_percent_rank_2") / F.nullif("element_rank_per_element_percent_rank_3", F.lit(0))).alias("element_percent_rank_2_per_element_rank_per_element_percent_rank_3"),
    )
)

filtered_price_tiers = (
    valid
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-03-17")))
    .select("*", F.upper("currency").alias("currency_upper"))
)

enriched_price_tiers = (
    filtered_price_tiers
    .withColumn("element_percent_rank_2_rank", F.rank().over(w8))
    .withColumn("element_percent_rank_2_per_element_rank_per_element_percent_rank_3_percent_rank", F.percent_rank().over(w9))
    .withColumn("element_percent_rank_percent_rank", F.percent_rank().over(w_running))
)

result = (
    enriched_price_tiers
    .select(
        "currency",
        F.col("element_percent_rank_percent_rank").alias("ELEMENT_PERCENT_RANK_PERCENT_RANK_VAL"),
        F.col("element_percent_rank_2_per_element_rank_per_element_percent_rank_3_percent_rank").alias("ELEMENT_PERCENT_RANK_2_PER_ELEMENT_RANK_PER_ELEMENT_PERCENT_RANK_3_PERCENT_RANK_VAL"),
        F.col("element").alias("ELEMENT_VAL"),
        "element_percent_rank_2",
    )
    .unpivot(
        ["currency"],
        ["ELEMENT_PERCENT_RANK_PERCENT_RANK_VAL", "ELEMENT_PERCENT_RANK_2_PER_ELEMENT_RANK_PER_ELEMENT_PERCENT_RANK_3_PERCENT_RANK_VAL", "ELEMENT_VAL", "element_percent_rank_2"],
        "metric_name",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
