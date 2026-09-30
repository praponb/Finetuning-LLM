from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "effective_from", F.explode("discounts").alias("val"))

result = (
    lines
    .groupBy(
        F.when(F.col("val") >= 64, "PREMIUM").otherwise("STANDARD").alias("val_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("val", 0.9).alias("val_p90"),
        F.count("*").alias("cnt"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("cnt").desc())
)
