from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("tier_limits").alias("val"))
)

result = (
    lines
    .groupBy("currency")
    .agg(
        F.round(F.avg("val"), 2).alias("avg_val"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("val_per_usd"),
        F.sum("val").alias("total_val"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
        F.sum("val").alias("total_val_2"),
    )
    .filter(F.col("num_currencies") > 1)
    .orderBy(F.col("val_per_usd").desc())
)
