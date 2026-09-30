from pyspark.sql import functions as F

lines = df_price_tiers.select("id", F.explode("tier_prices").alias("element"))

result = (
    lines
    .groupBy(
        F.when(F.col("element") >= 84, "HIGH").otherwise("LOW").alias("element_tier"),
    )
    .agg(
        F.round(F.avg("element"), 2).alias("avg_element"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("avg_element") > 0)
    .orderBy(F.col("avg_element").desc())
)
