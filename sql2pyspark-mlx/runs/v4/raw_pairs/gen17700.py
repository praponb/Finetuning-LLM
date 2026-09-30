from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("ends_on").desc())

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull() & F.col("promo_code").isin(["SPRING10", "VIP", "BUNDLE"]))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.concat_ws("-", "channel", "promo_code").alias("channel_promo_code_key"),
        F.upper("promo_code").alias("promo_code_upper"),
    )
)

ranked = (
    filtered
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

base = (
    ranked
    .filter((F.col("ends_on") >= F.lit("2023-11-17").cast("date")) & (F.col("rn") <= 1))
    .select(
        "*",
        F.dayofweek("ends_on").alias("ends_on_dayofweek"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

filtered_promotions = (
    base
    .filter(F.col("ends_on_dayofweek") < 69)
    .withColumn("channel_lower", F.lower("channel"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("N/A")))
)

result = (
    filtered_promotions
    .groupBy("channel")
    .pivot("promo_code", ["SPRING10", "BUNDLE"])
    .agg(F.sum("discount_pct"))
)
