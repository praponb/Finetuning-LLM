from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "currency",
        F.explode_outer("tier_prices").alias("tier_value"),
    )
)

valid = (
    lines
    .filter(F.col("tier_value") > 76)
    .withColumn("currency_length", F.length("currency"))
)

with_windows = (
    valid
    .filter(F.col("currency").isin(["THB", "USD"]))
    .withColumn("currency_max_currency_length", F.max("currency_length").over(w_ordered))
)

valid_price_tiers = (
    with_windows
    .filter(F.col("currency") == "USD")
    .select(
        "id",
        "plan",
        "currency",
        "tier_value",
        "currency_length",
        "currency_max_currency_length",
        (F.col("currency_length") * F.col("tier_value")).alias("currency_length_times_tier_value"),
        F.substring("currency", 1, 2).alias("currency_prefix"),
    )
)

cleaned = (
    valid_price_tiers
    .filter(F.col("currency_max_currency_length") == 66)
    .select(
        "currency",
        "currency_length",
        "currency_max_currency_length",
        "currency_prefix",
        F.length("currency").alias("currency_length_2"),
    )
)

cleaned_price_tiers = (
    cleaned
    .filter(F.col("currency") == "EUR")
    .withColumn("currency_length_2_minus_currency_length", (F.col("currency_length_2") - F.col("currency_length")))
)

result = (
    cleaned_price_tiers
    .select(
        "currency",
        "currency_max_currency_length",
        "currency_length_2",
        "currency_length_2_minus_currency_length",
        F.max("currency_length_2").over(w_ordered).alias("currency_max_currency_length_2"),
        F.stddev_samp("currency_length").over(w_ordered).alias("currency_stddev_samp_currency_length"),
        F.count("currency_max_currency_length").over(w_ordered).alias("currency_count_currency_max_currency_length"),
        F.sum("currency_length_2_minus_currency_length").over(w_ordered).alias("currency_sum_currency_length_2_minus_currency_length"),
    )
)
