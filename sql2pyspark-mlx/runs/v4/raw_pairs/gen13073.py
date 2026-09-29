from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("val")
w2 = Window.partitionBy("plan")
w_running = Window.partitionBy("plan", "currency")
w4 = Window.orderBy("val_percent_rank")
w5 = Window.partitionBy("currency", "plan")
w6 = Window.orderBy(F.col("currency_stddev_samp_val").asc_nulls_last())
w7 = Window.partitionBy("currency", "plan").orderBy(F.col("val").desc())

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("val"),
    )
)

metrics = (
    flattened
    .select(
        "*",
        F.percent_rank().over(w).alias("val_percent_rank"),
        F.avg("val").over(w2).alias("plan_avg_val"),
        F.max("val").over(w_running).alias("plan_max_val"),
    )
)

enriched = (
    metrics
    .withColumn("val_percent_rank_cume_dist", F.cume_dist().over(w4))
    .withColumn("plan_count_val", F.count("val").over(w2))
    .withColumn("currency_stddev_samp_val", F.stddev_samp("val").over(w5))
)

enriched_price_tiers = (
    enriched
    .withColumn("currency_stddev_samp_val_dense_rank", F.dense_rank().over(w6))
    .withColumn("val_cume_dist", F.cume_dist().over(w7))
)

result = (
    enriched_price_tiers
    .groupBy("plan", F.date_trunc("YEAR", "effective_from").alias("year_start"))
    .agg(
        F.percentile_approx("currency_stddev_samp_val_dense_rank", [0.25, 0.5, 0.75]).alias("currency_stddev_samp_val_dense_rank_quartiles"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("plan") == "starter", 1)).alias("starter_count"),
    )
    .orderBy(F.col("starter_count").desc_nulls_last())
)
