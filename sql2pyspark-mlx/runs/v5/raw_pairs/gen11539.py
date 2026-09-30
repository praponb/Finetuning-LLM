from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("element")

lines = df_price_tiers.select("id", "currency", F.explode("tier_prices").alias("element"))

scored = lines.select("*", F.percent_rank().over(w).alias("element_percent_rank"))

cleaned = (
    scored
    .filter(F.col("element_percent_rank") > 0.7)
    .withColumn("element_per_element_percent_rank", (F.col("element") / F.nullif("element_percent_rank", F.lit(0))))
    .withColumn("currency_prefix", F.substring("currency", 1, 2))
)

base = (
    cleaned
    .filter(F.col("currency") == "EUR")
    .withColumn("element_filled", F.coalesce("element", F.lit(0)))
)

result = (
    base
    .groupBy(
        F.when(F.col("element_per_element_percent_rank") >= 82, "LARGE").otherwise("SMALL").alias("element_per_element_percent_rank_tier"),
        "currency",
    )
    .agg(
        F.percentile_approx("element_filled", [0.25, 0.5, 0.75]).alias("element_filled_quartiles"),
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
    )
)
