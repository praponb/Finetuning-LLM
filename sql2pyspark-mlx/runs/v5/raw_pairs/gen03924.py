from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_limits").alias("element"),
    )
)

result = (
    exploded
    .groupBy("currency", F.date_trunc("YEAR", "effective_from").alias("year_start"))
    .agg(
        F.count_if(F.col("currency") == "THB").alias("thb_events"),
        F.sum(F.when(F.col("currency") == "EUR", F.col("element"))).alias("eur_total_element"),
        F.countDistinct("currency").alias("distinct_currencies"),
    )
    .filter(F.col("distinct_currencies") < 1000)
)
