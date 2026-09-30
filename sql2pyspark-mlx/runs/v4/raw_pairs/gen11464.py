from pyspark.sql import functions as F

base = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-12-22").cast("date")) & (F.year("starts_on") == 2023))
    .withColumn("ends_on_plus_months", F.add_months("ends_on", 6))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("N/A")))
)

result = (
    base
    .groupBy(
        F.when(F.col("discount_pct") >= 0.28, "LARGE").otherwise("SMALL").alias("discount_pct_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        F.round(F.sum("discount_pct") / F.nullif(F.count(F.when(F.col("channel") == "web", 1)), F.lit(0)), 2).alias("discount_pct_per_web"),
    )
)
