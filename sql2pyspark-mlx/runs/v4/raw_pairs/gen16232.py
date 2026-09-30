from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc())
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("val_rounded").asc_nulls_first())
w4 = Window.partitionBy("val_cume_dist_tier").orderBy(F.col("starter_events").desc())

exploded = df_price_tiers.select("id", "plan", F.explode("discounts").alias("val"))

base = (
    exploded
    .filter(F.col("plan").isin(["pro", "starter", "enterprise"]))
    .select("*", F.round("val", 2).alias("val_rounded"))
)

with_windows = (
    base
    .withColumn("val_cume_dist", F.cume_dist().over(w))
    .withColumn("plan_sum_val", F.sum("val").over(w2))
    .withColumn("val_rounded_rank", F.rank().over(w3))
)

grouped = (
    with_windows
    .groupBy(
        F.when(F.col("val_cume_dist") >= 0.62, "HIGH").otherwise("LOW").alias("val_cume_dist_tier"),
        F.when(F.col("val") >= 70, "LARGE").otherwise("SMALL").alias("val_tier"),
    )
    .agg(
        F.count_if(F.col("plan") == "starter").alias("starter_events"),
        F.round(F.avg("val_rounded"), 2).alias("avg_val_rounded"),
    )
    .withColumn("starter_events_dense_rank", F.dense_rank().over(w4))
)

result = (
    grouped
    .select("val_cume_dist_tier", "val_tier", "starter_events_dense_rank")
    .orderBy(F.col("val_cume_dist_tier").desc())
)
