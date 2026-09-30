from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "currency", F.explode("discounts").alias("val"))

valid = (
    lines
    .filter(F.col("val") >= 54)
    .withColumn("currency_group", F.when(F.col("currency").isin(["EUR", "USD"]), "Group A").otherwise("Other"))
)

base = (
    valid
    .filter(F.col("val") <= 32)
    .withColumn("currency_length", F.length("currency"))
)

stats = (
    base
    .withColumn("val_tier", F.when(F.col("val") >= 72, "LARGE").otherwise("SMALL"))
    .rollup("val_tier", "currency")
    .agg(
        F.sum(F.when(F.col("currency") == "USD", F.col("currency_length"))).alias("usd_total_currency_length"),
        F.count(F.when(F.col("currency") == "EUR", 1)).alias("eur_rows"),
        F.round(F.avg("val"), 2).alias("avg_val"),
        F.round(F.avg("currency_length"), 2).alias("avg_currency_length"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.size(F.collect_set("currency")).alias("num_currencies"),
        F.grouping("val_tier").alias("grouping_id"),
    )
)

result = (
    stats
    .select(
        "currency",
        "usd_total_currency_length",
        "eur_rows",
        "avg_val",
        "avg_currency_length",
        "currency_list",
        "grouping_id",
    )
    .orderBy(F.col("avg_currency_length").asc_nulls_first())
)
