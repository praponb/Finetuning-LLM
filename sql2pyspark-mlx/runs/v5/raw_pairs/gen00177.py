from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")

flattened = df_price_tiers.select("id", "currency", F.explode_outer("tier_prices").alias("val"))

enriched = (
    flattened
    .filter(F.col("val") < 63)
    .select(
        "id",
        "currency",
        F.stddev_samp("val").over(w).alias("currency_stddev_samp_val"),
    )
)

filtered = (
    enriched
    .filter(F.col("currency") == "USD")
    .select(
        "*",
        F.round((F.col("currency_stddev_samp_val") * 1.07), 2).alias("currency_stddev_samp_val_scaled"),
        F.round((F.col("currency_stddev_samp_val") * 0.85), 2).alias("currency_stddev_samp_val_adjusted"),
    )
)

cleaned = (
    filtered
    .filter(F.col("currency") == "EUR")
    .withColumn("currency_stddev_samp_val_adjusted_int", F.col("currency_stddev_samp_val_adjusted").cast("int"))
)

result = (
    cleaned
    .select(
        "id",
        "currency",
        "currency_stddev_samp_val",
        "currency_stddev_samp_val_scaled",
        "currency_stddev_samp_val_adjusted",
    )
)
