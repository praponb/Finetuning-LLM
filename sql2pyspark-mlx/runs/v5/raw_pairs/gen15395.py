from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("element").desc_nulls_first())
w3 = Window.orderBy(F.col("currency_count_element").desc_nulls_last())
w4 = Window.orderBy(F.col("currency_avg_element").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

base = (
    exploded
    .filter((F.col("effective_from") >= F.lit("2024-04-11").cast("date")) & (F.col("element") < 79))
    .withColumn("effective_from_plus_months", F.add_months("effective_from", 3))
)

enriched = (
    base
    .select(
        "effective_from",
        "effective_from_plus_months",
        F.count("element").over(w).alias("currency_count_element"),
        F.dense_rank().over(w2).alias("element_dense_rank"),
        F.avg("element").over(w).alias("currency_avg_element"),
    )
)

metrics = (
    enriched
    .filter(F.col("effective_from").between("2024-07-01", "2025-09-21"))
    .withColumn("currency_count_element_cume_dist", F.cume_dist().over(w3))
    .withColumn("currency_avg_element_rank", F.rank().over(w4))
)

result = (
    metrics
    .groupBy(F.date_trunc("WEEK", "effective_from").alias("week_start"))
    .agg(
        F.percentile_approx("currency_avg_element_rank", [0.25, 0.5, 0.75]).alias("currency_avg_element_rank_quartiles"),
        F.percentile_approx("currency_avg_element_rank", 0.5).alias("currency_avg_element_rank_p50"),
        F.percentile_approx("currency_avg_element_rank", 0.9).alias("currency_avg_element_rank_p90"),
    )
    .filter(F.col("currency_avg_element_rank_p90") > 0)
    .orderBy(F.col("currency_avg_element_rank_p50").asc_nulls_last())
)
