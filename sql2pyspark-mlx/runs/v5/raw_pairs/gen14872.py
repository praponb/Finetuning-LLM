from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc_nulls_first())
w2 = Window.partitionBy("plan").orderBy("val")
w3 = Window.partitionBy("plan", "currency")
w4 = Window.partitionBy("plan")
w5 = Window.partitionBy("currency").orderBy(F.col("val_rank").desc())
w6 = Window.orderBy(F.col("val_rank_2").desc())
w7 = Window.partitionBy("currency")
w8 = Window.orderBy(F.col("plan_max_plan_max_val").desc_nulls_first())
w9 = Window.orderBy(F.col("val_rank_2_percent_rank_scaled").asc_nulls_first())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

metrics = (
    lines
    .select(
        "*",
        F.rank().over(w).alias("val_rank"),
        F.rank().over(w2).alias("val_rank_2"),
        F.max("val").over(w3).alias("plan_max_val"),
    )
)

scored = (
    metrics
    .select(
        "effective_from",
        "currency",
        "val_rank_2",
        F.max("plan_max_val").over(w4).alias("plan_max_plan_max_val"),
        F.cume_dist().over(w5).alias("val_rank_cume_dist"),
        F.percent_rank().over(w6).alias("val_rank_2_percent_rank"),
        F.max("val").over(w7).alias("currency_max_val"),
        F.rank().over(w2).alias("val_rank_3"),
    )
)

base = (
    scored
    .filter(F.col("effective_from") < "2024-05-01")
    .select(
        "effective_from",
        "val_rank_2",
        "plan_max_plan_max_val",
        "val_rank_cume_dist",
        "currency_max_val",
        "val_rank_3",
        F.round((F.col("val_rank_2_percent_rank") * 1.15), 2).alias("val_rank_2_percent_rank_scaled"),
    )
)

enriched = (
    base
    .filter(F.col("currency_max_val") <= 59)
    .select(
        "effective_from",
        "val_rank_3",
        "val_rank_2_percent_rank_scaled",
        F.rank().over(w8).alias("plan_max_plan_max_val_rank"),
    )
)

metrics_price_tiers = (
    enriched
    .withColumn("val_rank_2_percent_rank_scaled_dense_rank", F.dense_rank().over(w9))
)

result = (
    metrics_price_tiers
    .cube(
        F.date_trunc("quarter", "effective_from").alias("quarter_start"),
        F.when(F.col("val_rank_2_percent_rank_scaled") >= 83, "HIGH").otherwise("LOW").alias("val_rank_2_percent_rank_scaled_tier"),
        F.date_trunc("week", "effective_from").alias("week_start"),
    )
    .agg(
        F.sum("val_rank_2_percent_rank_scaled_dense_rank").alias("total_val_rank_2_percent_rank_scaled_dense_rank"),
        F.percentile_approx("val_rank_2_percent_rank_scaled_dense_rank", 0.95).alias("val_rank_2_percent_rank_scaled_dense_rank_p95"),
        F.percentile_approx("val_rank_2_percent_rank_scaled", 0.9).alias("val_rank_2_percent_rank_scaled_p90"),
    )
    .orderBy(F.col("val_rank_2_percent_rank_scaled_tier").desc_nulls_first())
)
