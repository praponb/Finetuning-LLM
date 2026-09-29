from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("val"))
)

result = (
    exploded
    .groupingSets([["currency", "plan"], ["currency"]], "currency", "plan")
    .agg(
        F.round(F.avg("val"), 2).alias("avg_val"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("plan") == "basic", 1)), F.lit(0)), 2).alias("val_per_basic"),
    )
    .orderBy(F.col("avg_val").desc_nulls_last())
)
