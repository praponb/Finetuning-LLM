from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("element"))
)

result = (
    lines
    .groupBy(F.date_trunc("QUARTER", "effective_from").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.round(F.avg("element"), 2).alias("avg_element"),
    )
    .orderBy(F.col("avg_element").desc())
)
