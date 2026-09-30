from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("year_start").orderBy(F.col("eur_count").desc())

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("discounts").alias("tier_value"),
    )
)

result = (
    lines
    .groupBy(
        F.date_trunc("YEAR", "effective_from").alias("year_start"),
        F.when(F.col("tier_value") >= 73, "LARGE").otherwise("SMALL").alias("tier_value_tier"),
        "currency",
    )
    .agg(
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count"),
        F.sum(F.when(F.col("currency") == "THB", F.col("tier_value")).otherwise(0)).alias("thb_tier_value"),
        F.max("tier_value").alias("max_tier_value"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_count_2"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
    )
    .filter(F.col("max_tier_value") > 0)
    .withColumn("eur_count_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("eur_count_2").desc_nulls_first())
)
