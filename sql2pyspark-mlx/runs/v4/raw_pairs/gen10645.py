from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan", "currency").orderBy(F.col("val").desc())
w3 = Window.partitionBy("plan").orderBy(F.col("plan_min_val").asc_nulls_last())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("val"),
    )
)

metrics = (
    lines
    .withColumn("plan_min_val", F.min("val").over(w))
    .withColumn("val_dense_rank", F.dense_rank().over(w2))
)

scored = (
    metrics
    .select(
        "*",
        F.cume_dist().over(w3).alias("plan_min_val_cume_dist"),
        F.max("val_dense_rank").over(w).alias("plan_max_val_dense_rank"),
    )
)

filtered = (
    scored
    .filter(F.col("effective_from") > F.to_date(F.lit("2024-09-23")))
    .withColumn("currency_plan", F.concat("currency", F.lit(" - "), "plan"))
    .withColumn("val_band", F.when(F.col("val") >= 64, "High").when(F.col("val") >= 30, "Medium").otherwise("Low"))
)

filtered_price_tiers = (
    filtered
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-09-18"))) & (F.col("plan_max_val_dense_rank") > 4))
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        "val",
        "val_dense_rank",
        "plan_min_val_cume_dist",
        "currency_plan",
        "val_band",
        F.initcap(F.trim("plan")).alias("plan_title"),
    )
)

result = (
    filtered_price_tiers
    .groupBy("currency", "plan")
    .agg(
        F.round(F.sum("plan_min_val_cume_dist") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("plan_min_val_cume_dist_per_eur"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.round(F.avg("plan_min_val_cume_dist"), 2).alias("avg_plan_min_val_cume_dist"),
        F.round(F.avg("plan_min_val_cume_dist"), 2).alias("avg_plan_min_val_cume_dist_2"),
        F.avg("val_dense_rank").alias("avg_val_dense_rank"),
    )
    .orderBy(F.col("avg_plan_min_val_cume_dist").asc_nulls_first())
)
