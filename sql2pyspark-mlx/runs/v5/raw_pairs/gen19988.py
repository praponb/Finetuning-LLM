from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_first())
w2 = Window.partitionBy("currency")
w3 = Window.orderBy(F.col("element_rank").desc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("element").asc_nulls_last())
w5 = Window.orderBy(F.col("element_rank").desc())
w6 = Window.partitionBy("currency", "plan").orderBy(F.col("element_rank").desc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("element"),
    )
)

with_windows = (
    lines
    .filter(F.col("currency") == "EUR")
    .withColumn("element_rank", F.rank().over(w_running))
)

enriched = (
    with_windows
    .select(
        "effective_from",
        "currency",
        F.stddev_samp("element_rank").over(w2).alias("currency_stddev_samp_element_rank"),
        F.cume_dist().over(w3).alias("element_rank_cume_dist"),
        F.rank().over(w4).alias("element_rank_2"),
        F.percent_rank().over(w5).alias("element_rank_percent_rank"),
        F.dense_rank().over(w6).alias("element_rank_dense_rank"),
    )
)

cleaned = (
    enriched
    .filter((F.col("effective_from") >= F.lit("2024-03-24").cast("date")) & (F.col("element_rank_dense_rank") >= 3))
    .withColumn("element_rank_2_str", F.col("element_rank_2").cast("double"))
    .withColumn("least_element_rank_percent_rank_currency_stddev_samp_element_rank", F.least("element_rank_percent_rank", "currency_stddev_samp_element_rank"))
)

result = (
    cleaned
    .select(
        "effective_from",
        "currency",
        "currency_stddev_samp_element_rank",
        "element_rank_cume_dist",
        "element_rank_dense_rank",
        "element_rank_2_str",
        F.when(F.col("currency").rlike("^(USD|EUR)$"), "CORE").when(F.col("currency").rlike("^(THB)$"), "GROUP_A").otherwise("PRIORITY").alias("currency_zone"),
    )
    .orderBy(F.col("element_rank_dense_rank").desc_nulls_last())
)
