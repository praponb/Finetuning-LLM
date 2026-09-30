from pyspark.sql import functions as F

lines = df_price_tiers.select("id", F.explode("tier_prices").alias("element"))

result = (
    lines
    .groupBy(
        F.when(F.col("element") >= 74, "LARGE").otherwise("SMALL").alias("element_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
    )
)
