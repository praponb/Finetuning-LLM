from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "currency", F.explode("tier_prices").alias("element"))

result = (
    lines
    .cube(
        F.when(F.col("element") >= 63, "LARGE").otherwise("SMALL").alias("element_tier"),
    )
    .agg(
        F.count_if(F.col("currency") == "USD").alias("usd_events"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.avg(F.when(F.col("currency") == "THB", F.col("element"))).alias("thb_avg_element"),
    )
    .orderBy(F.col("element_tier").desc_nulls_last())
)
