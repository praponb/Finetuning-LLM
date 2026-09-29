from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("year_start").orderBy(F.col("thb_total_element").desc())

lines = (
    df_price_tiers
    .select("id", "effective_from", "currency", F.explode("discounts").alias("element"))
)

result = (
    lines
    .groupBy(
        F.date_trunc("year", "effective_from").alias("year_start"),
        F.when(F.col("element") >= 64, "HIGH").otherwise("LOW").alias("element_tier"),
        F.date_trunc("week", "effective_from").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("element_per_usd"),
        F.sum(F.when(F.col("currency") == "THB", F.col("element"))).alias("thb_total_element"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("element_per_usd_2"),
    )
    .filter(F.col("element_per_usd") < 1000000)
    .withColumn("thb_total_element_percent_rank", F.percent_rank().over(w))
    .orderBy("element_per_usd_2")
)
