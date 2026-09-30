from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency")
w2 = Window.partitionBy("plan").orderBy(F.col("val").desc_nulls_last())
w3 = Window.partitionBy("currency").orderBy(F.col("val").desc())
w4 = Window.partitionBy("plan")
w5 = Window.partitionBy("currency", "plan").orderBy(F.col("val").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("tier_limits").alias("val"),
    )
)

enriched = (
    lines
    .withColumn("plan_avg_val", F.avg("val").over(w))
    .withColumn("val_cume_dist", F.cume_dist().over(w2))
    .withColumn("val_percent_rank", F.percent_rank().over(w3))
)

cleaned = (
    enriched
    .filter(F.col("val_cume_dist").between(0.59, 0.77))
    .withColumn("currency_plan_key", F.concat_ws("-", "currency", "plan"))
)

scored = (
    cleaned
    .select(
        "*",
        F.min("val").over(w4).alias("plan_min_val"),
        F.dense_rank().over(w5).alias("val_dense_rank"),
    )
)

result = (
    scored
    .cube(F.date_trunc("QUARTER", "effective_from").alias("quarter_start"), "currency")
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.percentile_approx("val_percent_rank", [0.25, 0.5, 0.75]).alias("val_percent_rank_quartiles"),
    )
    .orderBy(F.col("basic_rows").asc_nulls_last())
)
