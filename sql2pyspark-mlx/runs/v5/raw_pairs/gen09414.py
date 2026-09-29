from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("val_tier").orderBy(F.col("avg_val").desc())

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode("tier_limits").alias("val"))
)

result = (
    exploded
    .groupBy(
        F.when(F.col("val") >= 84, "PREMIUM").otherwise("STANDARD").alias("val_tier"),
        "currency",
    )
    .agg(
        F.round(F.avg("val"), 2).alias("avg_val"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.percentile_approx("val", 0.5).alias("val_p50"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("currency") == "THB", 1)), F.lit(0)), 2).alias("val_per_thb"),
        F.sum(F.when(F.col("currency") == "USD", F.col("val")).otherwise(0)).alias("usd_val"),
    )
    .withColumn("avg_val_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("val_per_thb").desc_nulls_first())
)
