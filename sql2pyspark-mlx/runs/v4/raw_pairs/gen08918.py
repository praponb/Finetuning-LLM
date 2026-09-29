from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("element").asc_nulls_last())
w_recent = Window.partitionBy("currency")
w3 = Window.partitionBy("currency").orderBy(F.col("element").desc_nulls_last())
w4 = Window.partitionBy("currency").orderBy(F.col("element_percent_rank").desc_nulls_first())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode("tier_prices").alias("element"),
    )
)

scored = (
    exploded
    .select(
        "*",
        F.rank().over(w_grp).alias("element_rank"),
        F.min("element").over(w_recent).alias("currency_min_element"),
        F.avg("element").over(w_recent).alias("currency_avg_element"),
        F.percent_rank().over(w3).alias("element_percent_rank"),
    )
)

remaining_keys = (
    scored
    .select("id")
    .exceptAll(
        df_price_tiers
        .filter(F.year("effective_from") == 2024)
        .select("id")
    )
)

verified = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-06-10"))) & (F.col("currency_min_element") <= 40))
    .withColumn("adjusted_currency_avg_element", F.round((F.col("currency_avg_element") * 1.1), 2))
)

enriched = (
    cleaned
    .select(
        "*",
        F.dense_rank().over(w4).alias("element_percent_rank_dense_rank"),
        F.min("adjusted_currency_avg_element").over(w_recent).alias("currency_min_adjusted_currency_avg_element"),
        F.stddev_samp("currency_min_element").over(w_recent).alias("currency_stddev_samp_currency_min_element"),
    )
)

remaining_keys_2 = (
    enriched
    .select("id", "currency")
    .subtract(
        df_price_tiers
        .filter(F.col("plan").isin(["enterprise", "starter"]))
        .select("id", "currency")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.currency") == F.col("k.currency")), "inner")
    .select("s.*")
)

cleaned_price_tiers = (
    kept_rows
    .filter(F.col("currency_min_element").isNotNull())
    .select(
        "*",
        F.coalesce("currency_min_element", F.lit(0)).alias("currency_min_element_filled"),
    )
)

result = (
    cleaned_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        "element",
        "currency_min_element",
        "currency_avg_element",
        "element_percent_rank_dense_rank",
        "currency_min_adjusted_currency_avg_element",
        "currency_stddev_samp_currency_min_element",
        F.when(F.col("currency").rlike("^(USD|THB)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("currency_zone"),
    )
)
