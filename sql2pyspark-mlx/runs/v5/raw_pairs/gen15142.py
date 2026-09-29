from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select("id", "effective_from", F.explode("tier_limits").alias("tier_value"))
)

result = (
    flattened
    .groupBy(
        F.when(F.col("tier_value") >= 71, "HIGH").otherwise("LOW").alias("tier_value_tier"),
        F.date_trunc("WEEK", "effective_from").alias("week_start"),
        F.date_trunc("MONTH", "effective_from").alias("activity_month"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("cnt_2"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy(F.col("cnt").desc())
)
