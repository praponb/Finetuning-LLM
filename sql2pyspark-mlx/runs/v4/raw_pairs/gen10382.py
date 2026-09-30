from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_limits").alias("val"))
)

result = (
    flattened
    .groupBy(F.when(F.col("val") >= 83, "LARGE").otherwise("SMALL").alias("val_tier"))
    .agg(
        F.avg(F.when(F.col("plan") == "enterprise", F.col("val"))).alias("enterprise_avg_val"),
        F.max(F.when(F.col("currency") == "EUR", F.col("val"))).alias("eur_max_val"),
    )
    .filter(F.col("enterprise_avg_val") < 0)
    .orderBy("enterprise_avg_val")
)
