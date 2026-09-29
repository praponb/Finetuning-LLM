from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency").orderBy(F.col("val").asc_nulls_first())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_first())
w4 = Window.partitionBy("currency").orderBy(F.col("greatest_val_val_percent_rank_2").asc_nulls_last())
w5 = Window.partitionBy("currency")
w6 = Window.partitionBy("currency").orderBy(F.col("val_percent_rank").asc_nulls_last())
w7 = Window.partitionBy("currency").orderBy(F.col("val_percent_rank_dense_rank").asc_nulls_first())
w8 = Window.partitionBy("currency").orderBy(F.col("val_percent_rank_dense_rank").desc_nulls_last())

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("val"),
    )
)

with_windows = (
    exploded
    .withColumn("val_percent_rank", F.percent_rank().over(w))
    .withColumn("plan_count_val", F.count("val").over(w2))
    .withColumn("val_percent_rank_2", F.percent_rank().over(w3))
)

filtered = (
    with_windows
    .filter(F.col("effective_from") >= F.lit("2024-04-30").cast("date"))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "val",
        "val_percent_rank",
        "plan_count_val",
        "val_percent_rank_2",
        F.greatest("val", "val_percent_rank_2").alias("greatest_val_val_percent_rank_2"),
        F.when(F.col("plan_count_val") >= 16, "High").when(F.col("plan_count_val") >= 7, "Medium").otherwise("Low").alias("plan_count_val_level"),
    )
)

cleaned = (
    filtered
    .filter(F.col("effective_from") >= F.lit("2024-02-22").cast("date"))
    .select(
        "id",
        "effective_from",
        "currency",
        "val_percent_rank",
        "val_percent_rank_2",
        "greatest_val_val_percent_rank_2",
        F.ceil("val").alias("val_whole"),
    )
)

scored = (
    cleaned
    .filter(F.col("val_percent_rank_2").between(0.56, 0.72))
    .select(
        "id",
        "currency",
        "greatest_val_val_percent_rank_2",
        "val_whole",
        F.percent_rank().over(w4).alias("greatest_val_val_percent_rank_2_percent_rank"),
        F.sum("val_whole").over(w5).alias("currency_sum_val_whole"),
        F.count("greatest_val_val_percent_rank_2").over(w5).alias("currency_count_greatest_val_val_percent_rank_2"),
        F.dense_rank().over(w6).alias("val_percent_rank_dense_rank"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.sum("greatest_val_val_percent_rank_2").over(w5).alias("currency_sum_greatest_val_val_percent_rank_2"),
        F.percent_rank().over(w7).alias("val_percent_rank_dense_rank_percent_rank"),
        F.rank().over(w8).alias("val_percent_rank_dense_rank_rank"),
    )
)

stats = (
    enriched
    .groupBy(
        F.when(F.col("val_percent_rank_dense_rank_rank") >= 9, "PREMIUM").otherwise("STANDARD").alias("val_percent_rank_dense_rank_rank_tier"),
    )
    .agg(
        F.count_if(F.col("currency") == "EUR").alias("eur_events"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
    )
    .filter(F.col("eur_events") < 1000)
)

result = stats.select("val_percent_rank_dense_rank_rank_tier", "eur_events", "currency_list")
