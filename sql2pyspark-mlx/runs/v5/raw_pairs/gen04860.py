from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

result = (
    lines
    .cube("currency")
    .agg(
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.grouping("currency").alias("grouping_id"),
    )
    .orderBy(F.col("currency").asc_nulls_last())
)
