from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", F.explode("tier_prices").alias("element"))

valid = (
    flattened
    .filter(F.col("element") < 68)
    .select("*", F.round((F.col("element") * 100), 2).alias("adjusted_element"))
)

valid_price_tiers = (
    valid
    .filter(F.col("adjusted_element") <= 40)
    .withColumn("adjusted_element_plus_element", (F.col("adjusted_element") + F.col("element")))
)

valid_price_tiers_2 = (
    valid_price_tiers
    .filter(F.col("adjusted_element").between(58, 59))
    .select("*", F.col("adjusted_element").cast("int").alias("adjusted_element_int"))
)

result = (
    valid_price_tiers_2
    .groupBy(
        F.when(F.col("adjusted_element_plus_element") >= 78, "HIGH").otherwise("LOW").alias("adjusted_element_plus_element_tier"),
        F.when(F.col("adjusted_element") >= 64, "PREMIUM").otherwise("STANDARD").alias("adjusted_element_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
    )
    .orderBy(F.col("event_count").desc())
)
