from pyspark.sql import functions as F

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("element"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("QUARTER", "effective_from").alias("quarter_start"))
    .agg(
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.countDistinct("currency").alias("distinct_currencies"),
        F.round(F.avg("element"), 2).alias("avg_element"),
        F.round(F.sum("element") / F.nullif(F.count(F.when(F.col("currency") == "EUR", 1)), F.lit(0)), 2).alias("element_per_eur"),
    )
    .filter(F.col("avg_element") > 0)
    .orderBy(F.col("distinct_currencies").desc())
)
