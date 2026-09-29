from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode_outer("tier_prices").alias("val"))
)

cleaned = (
    flattened
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-06-17"))) & F.col("val").between(54, 58))
    .select("*", F.round((F.col("val") * 0.9), 2).alias("adjusted_val"))
)

cleaned_price_tiers = (
    cleaned
    .filter((F.col("effective_from") > F.lit("2024-08-09").cast("date")) & (F.col("val") > 61))
    .select(
        "plan",
        "effective_from",
        "val",
        "adjusted_val",
        F.round((F.col("adjusted_val") * 1.07), 2).alias("adjusted_val_adjusted"),
    )
)

result = (
    cleaned_price_tiers
    .withColumn("activity_month", F.date_trunc("MONTH", "effective_from"))
    .groupingSets([["plan", "activity_month"], ["plan"]], "plan", "activity_month")
    .agg(
        F.count_if(F.col("plan") == "pro").alias("pro_events"),
        F.round(F.avg("adjusted_val_adjusted"), 2).alias("avg_adjusted_val_adjusted"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.avg(F.when(F.col("plan") == "starter", F.col("adjusted_val"))).alias("starter_avg_adjusted_val"),
        F.round(F.avg("adjusted_val"), 2).alias("avg_adjusted_val"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("pro_events").desc())
)
