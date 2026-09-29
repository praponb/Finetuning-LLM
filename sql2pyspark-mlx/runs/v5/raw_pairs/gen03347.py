from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("discounts").alias("val"))
)

result = (
    lines
    .groupingSets([["plan", "currency"], ["plan"]], "plan", "currency")
    .agg(
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.sum(F.when(F.col("currency") == "THB", F.col("val"))).alias("thb_total_val"),
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.min("val").alias("min_val"),
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles_2"),
    )
    .orderBy(F.col("plan").desc_nulls_first())
)
