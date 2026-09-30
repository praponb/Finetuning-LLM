from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("element").desc_nulls_last())
w2 = Window.orderBy(F.col("element").desc())
w3 = Window.orderBy(F.col("element").asc_nulls_first())
w4 = Window.orderBy(F.col("element_cume_dist").desc_nulls_last())
w5 = Window.orderBy(F.col("element_rounded").desc_nulls_first())
w6 = Window.orderBy(F.col("element_rounded").desc_nulls_last())
w7 = Window.orderBy(F.col("element_rounded_percent_rank").desc())
w8 = Window.orderBy(F.col("element_rounded_rank").desc())
w9 = Window.orderBy(F.col("element_cume_dist_2").asc_nulls_last())
w_grp = Window.orderBy(F.col("element_rounded_percent_rank").desc_nulls_first())
w11 = Window.orderBy("element_rounded_percent_rank")
w12 = Window.orderBy(F.col("element_rounded_rank").desc_nulls_last())
w13 = Window.orderBy(F.col("element_cume_dist_rank").desc_nulls_last())
w14 = Window.orderBy(F.col("element_rounded_rank_rank").desc_nulls_last())
w_running = Window.orderBy(F.col("element_rounded").desc())
w16 = Window.orderBy(F.col("element_rounded_percent_rank_rank_2").desc())
w17 = Window.orderBy("element_rounded")

exploded = df_price_tiers.select("id", F.explode("tier_limits").alias("element"))

enriched = (
    exploded
    .withColumn("element_cume_dist", F.cume_dist().over(w))
    .withColumn("element_cume_dist_2", F.cume_dist().over(w2))
    .withColumn("element_percent_rank", F.percent_rank().over(w3))
)

base = (
    enriched
    .filter(F.col("element_cume_dist") < 0.22)
    .withColumn("element_rounded", F.round("element", 2))
)

scored = (
    base
    .select(
        "*",
        F.rank().over(w4).alias("element_cume_dist_rank"),
        F.rank().over(w5).alias("element_rounded_rank"),
        F.percent_rank().over(w6).alias("element_rounded_percent_rank"),
    )
)

enriched_price_tiers = (
    scored
    .select(
        "*",
        F.rank().over(w7).alias("element_rounded_percent_rank_rank"),
        F.rank().over(w8).alias("element_rounded_rank_rank"),
        F.percent_rank().over(w9).alias("element_cume_dist_2_percent_rank"),
        F.rank().over(w_grp).alias("element_rounded_percent_rank_rank_2"),
    )
)

scored_price_tiers = (
    enriched_price_tiers
    .select(
        "*",
        F.cume_dist().over(w11).alias("element_rounded_percent_rank_cume_dist"),
        F.percent_rank().over(w12).alias("element_rounded_rank_percent_rank"),
        F.cume_dist().over(w13).alias("element_cume_dist_rank_cume_dist"),
    )
)

metrics = (
    scored_price_tiers
    .select(
        "*",
        F.cume_dist().over(w14).alias("element_rounded_rank_rank_cume_dist"),
        F.percent_rank().over(w_running).alias("element_rounded_percent_rank_2"),
        F.cume_dist().over(w16).alias("element_rounded_percent_rank_rank_2_cume_dist"),
        F.percent_rank().over(w_running).alias("element_rounded_percent_rank_3"),
        F.cume_dist().over(w17).alias("element_rounded_cume_dist"),
    )
)

report = (
    metrics
    .select(
        "id",
        "element",
        "element_cume_dist",
        "element_cume_dist_2",
        "element_percent_rank",
        "element_rounded",
        "element_rounded_rank",
        "element_rounded_percent_rank",
        "element_rounded_percent_rank_rank",
    )
    .orderBy(F.col("element_rounded_percent_rank_rank").desc_nulls_last())
)

result = (
    report
    .select(
        "element",
        "element_percent_rank",
        "element_rounded",
        "element_rounded_percent_rank",
    )
    .orderBy(F.col("element_percent_rank").desc_nulls_last(), "element_rounded")
)
