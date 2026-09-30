from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.partitionBy("promo_code").orderBy("rn", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("channel").orderBy(F.col("ends_on").desc(), "id")

valid = (
    df_promotions
    .filter(F.col("discount_pct").between(0.15, 0.2))
    .withColumn("days_since_ends_on", F.datediff(F.current_date(), "ends_on"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("UNKNOWN")))
)

cleaned = (
    valid
    .filter((F.col("starts_on") > F.to_date(F.lit("2023-08-17"))) & F.col("channel").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "starts_on").alias("days_since_starts_on"),
        F.when(F.col("channel").isin(["partner", "store"]), "Core").otherwise("Other").alias("channel_group"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_filled_2"),
    )
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.avg("days_since_ends_on").over(w2).alias("days_since_ends_on_avg_6_prior"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

result = (
    scored
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "discount_pct",
        "days_since_ends_on",
        "channel_filled",
        "channel_group",
        "channel_filled_2",
    )
    .orderBy(F.col("discount_pct").asc_nulls_first())
)
