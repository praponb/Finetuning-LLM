from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("currency_sum_val_whole").desc_nulls_last())
w3 = Window.orderBy(F.col("currency_sum_val_whole").desc_nulls_first())
w_grp_2 = Window.orderBy(F.col("val_whole_minus_currency_min_val_whole").desc())

exploded = df_price_tiers.select("id", "plan", "currency", F.explode("discounts").alias("val"))

valid = (
    exploded
    .filter(F.col("plan").isin(["enterprise", "basic"]))
    .select(
        "*",
        F.ceil("val").alias("val_whole"),
        F.substring("currency", 1, 4).alias("currency_prefix"),
    )
)

valid_price_tiers = (
    valid
    .filter(F.col("val_whole") > 43)
    .select(
        "currency",
        "val_whole",
        "currency_prefix",
        F.initcap(F.trim("plan")).alias("plan_title"),
        F.upper("plan").alias("plan_upper"),
    )
)

enriched = (
    valid_price_tiers
    .select(
        "val_whole",
        "currency_prefix",
        "plan_title",
        F.min("val_whole").over(w_grp).alias("currency_min_val_whole"),
        F.sum("val_whole").over(w_grp).alias("currency_sum_val_whole"),
    )
)

valid_price_tiers_2 = (
    enriched
    .filter(F.col("val_whole") <= 39)
    .withColumn("currency_sum_val_whole_tier", F.when(F.col("currency_sum_val_whole") >= 341, "High").when(F.col("currency_sum_val_whole") >= 126, "Medium").otherwise("Low"))
)

valid_price_tiers_3 = (
    valid_price_tiers_2
    .filter(F.col("val_whole") > 32)
    .select(
        "val_whole",
        "plan_title",
        "currency_min_val_whole",
        "currency_sum_val_whole",
        (F.col("val_whole") - F.col("currency_min_val_whole")).alias("val_whole_minus_currency_min_val_whole"),
    )
)

metrics = (
    valid_price_tiers_3
    .select(
        "val_whole",
        "plan_title",
        F.cume_dist().over(w2).alias("currency_sum_val_whole_cume_dist"),
        F.rank().over(w2).alias("currency_sum_val_whole_rank"),
        F.dense_rank().over(w3).alias("currency_sum_val_whole_dense_rank"),
        F.percent_rank().over(w_grp_2).alias("val_whole_minus_currency_min_val_whole_percent_rank"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("val_whole_minus_currency_min_val_whole_percent_rank") >= 0.85, "PREMIUM").otherwise("STANDARD").alias("val_whole_minus_currency_min_val_whole_percent_rank_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("currency_sum_val_whole_rank"), 2).alias("avg_currency_sum_val_whole_rank"),
        F.min("currency_sum_val_whole_rank").alias("min_currency_sum_val_whole_rank"),
        F.count("*").alias("num_rows_2"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("min_currency_sum_val_whole_rank").desc_nulls_first())
)
