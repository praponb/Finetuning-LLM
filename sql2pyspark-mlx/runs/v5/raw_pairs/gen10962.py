from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_first())
w2 = Window.partitionBy("plan").orderBy(F.col("val").asc_nulls_last())

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("discounts").alias("val"))
)

base = (
    flattened
    .filter(F.col("plan").isin(["basic", "pro", "starter"]))
    .select("*", F.trim("plan").alias("clean_plan"))
)

base_price_tiers = (
    base
    .filter((F.col("effective_from") >= F.lit("2024-03-11").cast("date")) & (F.col("effective_from") >= "2025-05-01"))
    .withColumn("effective_from_label", F.date_format("effective_from", "yyyy-MM"))
)

with_windows = (
    base_price_tiers
    .withColumn("val_cume_dist", F.cume_dist().over(w))
    .withColumn("val_cume_dist_2", F.cume_dist().over(w2))
    .withColumn("val_dense_rank", F.dense_rank().over(w2))
)

result = (
    with_windows
    .withColumn("val_cume_dist_tier", F.when(F.col("val_cume_dist") >= 0.71, "LARGE").otherwise("SMALL"))
    .cube("val_cume_dist_tier", "plan")
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.percentile_approx("val_dense_rank", [0.25, 0.5, 0.75]).alias("val_dense_rank_quartiles"),
        F.count("*").alias("event_count"),
        F.count_if(F.col("plan") == "starter").alias("starter_events"),
        F.grouping("val_cume_dist_tier").alias("grouping_level"),
    )
)
