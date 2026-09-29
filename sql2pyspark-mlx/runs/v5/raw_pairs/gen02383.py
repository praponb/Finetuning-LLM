from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("plan", "currency").orderBy(F.col("val").desc_nulls_last())
w3 = Window.orderBy(F.col("val").desc_nulls_last())
w4 = Window.partitionBy("plan").orderBy(F.col("plan_min_val").desc_nulls_first())
w5 = Window.orderBy(F.col("plan_min_val").desc_nulls_first())
w_part = Window.partitionBy("currency").orderBy(F.col("plan_max_val").desc())

lines = df_price_tiers.select("id", "plan", "currency", F.explode("discounts").alias("val"))

enriched = (
    lines
    .filter(F.col("plan") == "basic")
    .withColumn("plan_min_val", F.min("val").over(w))
    .withColumn("val_dense_rank", F.dense_rank().over(w2))
    .withColumn("val_cume_dist", F.cume_dist().over(w3))
    .withColumn("plan_max_val", F.max("val").over(w))
)

metrics = (
    enriched
    .select(
        "*",
        F.cume_dist().over(w4).alias("plan_min_val_cume_dist"),
        F.dense_rank().over(w5).alias("plan_min_val_dense_rank"),
        F.percent_rank().over(w_part).alias("plan_max_val_percent_rank"),
    )
)

base = (
    metrics
    .filter(F.col("plan") == "enterprise")
    .select("*", F.col("val").cast("int").alias("val_int"))
)

result = (
    base
    .groupBy(
        F.when(F.col("plan_min_val_cume_dist") >= 0.81, "HIGH").otherwise("LOW").alias("plan_min_val_cume_dist_tier"),
        F.when(F.col("plan_max_val_percent_rank") >= 0.86, "HIGH").otherwise("LOW").alias("plan_max_val_percent_rank_tier"),
    )
    .agg(
        F.percentile_approx("plan_min_val_dense_rank", 0.5).alias("plan_min_val_dense_rank_p50"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.sum("val_cume_dist").alias("total_val_cume_dist"),
        F.sum("plan_min_val").alias("total_plan_min_val"),
        F.percentile_approx("val_dense_rank", [0.25, 0.5, 0.75]).alias("val_dense_rank_quartiles"),
    )
    .orderBy(F.col("plan_min_val_cume_dist_tier").desc_nulls_last())
)
