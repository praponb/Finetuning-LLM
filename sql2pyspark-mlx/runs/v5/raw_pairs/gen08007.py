from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", F.explode_outer("tier_prices").alias("element"))

result = (
    flattened
    .groupBy(
        F.when(F.col("element") >= 78, "PREMIUM").otherwise("STANDARD").alias("element_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.min("element").alias("min_element"),
    )
    .filter(F.col("num_rows") >= 1)
    .orderBy(F.col("distinct_ids_2").asc_nulls_last())
)
