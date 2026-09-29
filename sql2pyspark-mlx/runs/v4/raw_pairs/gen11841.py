from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("val"))
)

result = (
    lines
    .groupBy(
        F.when(F.col("val") >= 88, "PREMIUM").otherwise("STANDARD").alias("val_tier"),
    )
    .agg(
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("plan") == "starter", 1)), F.lit(0)), 2).alias("val_per_starter"),
        F.round(F.avg("val"), 2).alias("avg_val"),
        F.count_if(F.col("currency") == "THB").alias("thb_events"),
        F.sum("val").alias("total_val"),
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("val_per_eur"),
        F.round(F.avg("val"), 2).alias("avg_val_2"),
    )
    .filter(F.col("total_val") < 0)
    .orderBy(F.col("avg_val").desc())
)
