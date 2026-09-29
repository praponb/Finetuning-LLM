from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "currency", F.explode_outer("tier_limits").alias("val"))

cleaned = (
    flattened
    .filter(F.col("val") > 37)
    .withColumn("currency_upper", F.upper("currency"))
)

cleaned_price_tiers = (
    cleaned
    .filter(F.col("val").between(61, 70))
    .withColumn("clean_currency", F.trim("currency"))
)

base = (
    cleaned_price_tiers
    .filter(F.col("val") > 65)
    .select(
        "id",
        "currency",
        "val",
        "clean_currency",
        F.upper("currency").alias("currency_upper_2"),
        F.ceil("val").alias("val_whole"),
    )
)

base_price_tiers = (
    base
    .filter(F.col("currency") == "USD")
    .select(
        "val",
        "clean_currency",
        "currency_upper_2",
        "val_whole",
        (F.col("val") * F.col("val_whole")).alias("val_times_val_whole"),
    )
)

result = base_price_tiers.select("clean_currency", "val_whole")
