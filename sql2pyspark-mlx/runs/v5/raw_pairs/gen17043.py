from pyspark.sql import functions as F

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

result = (
    flattened
    .rollup(F.date_trunc("week", "effective_from").alias("week_start"))
    .agg(
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_count"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") < 1000)
    .orderBy(F.col("event_count").desc())
)
