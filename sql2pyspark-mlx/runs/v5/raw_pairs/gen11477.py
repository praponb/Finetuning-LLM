from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("plan", "currency")
w2 = Window.partitionBy("currency")
w_hist = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_first())
w4 = Window.partitionBy("currency").orderBy("plan_count_val")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

enriched = (
    flattened
    .select(
        "id",
        "effective_from",
        "currency",
        "val",
        F.count("val").over(w_part).alias("plan_count_val"),
        F.max("val").over(w_part).alias("plan_max_val"),
        F.count("val").over(w_part).alias("plan_count_val_2"),
        F.sum("val").over(w2).alias("currency_sum_val"),
        F.percent_rank().over(w_hist).alias("val_percent_rank"),
    )
)

valid = (
    enriched
    .filter((F.col("effective_from") >= F.lit("2024-02-17").cast("date")) & (F.col("val") < 75))
    .select(
        "*",
        F.when(F.col("val") >= 61, "High").when(F.col("val") >= 25, "Medium").otherwise("Low").alias("val_band"),
    )
)

with_windows = (
    valid
    .select(
        "effective_from",
        "currency",
        "val",
        "plan_count_val",
        "plan_count_val_2",
        "val_percent_rank",
        "val_band",
        F.avg("val_percent_rank").over(w2).alias("currency_avg_val_percent_rank"),
        F.min("plan_max_val").over(w2).alias("currency_min_plan_max_val"),
        F.cume_dist().over(w4).alias("plan_count_val_cume_dist"),
    )
)

metrics = (
    with_windows
    .select(
        "effective_from",
        "currency",
        "val",
        "plan_count_val",
        "val_percent_rank",
        "plan_count_val_cume_dist",
        F.sum("val_percent_rank").over(w2).alias("currency_sum_val_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "effective_from",
        "currency",
        "val",
        "plan_count_val",
        "val_percent_rank",
        "plan_count_val_cume_dist",
        "currency_sum_val_percent_rank",
        F.when(F.col("currency").rlike("^(THB|USD)$"), "GROUP_A").otherwise("CORE").alias("currency_zone"),
    )
)
