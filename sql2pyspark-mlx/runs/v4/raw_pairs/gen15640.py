from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "currency", F.explode("discounts").alias("element"))

result = (
    flattened
    .cube("currency")
    .agg(
        F.sum(F.when(F.col("currency") == "USD", F.col("element"))).alias("usd_total_element"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.grouping("currency").alias("grouping_id"),
    )
)
