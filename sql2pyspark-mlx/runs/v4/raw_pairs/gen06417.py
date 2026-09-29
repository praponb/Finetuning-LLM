from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("ends_on", "id")
w2 = Window.orderBy(F.col("ends_on").desc(), "id")
w3 = Window.orderBy("starts_on", "id")
w4 = Window.partitionBy("promotion_promo_code").orderBy("event_seq_2", "id", "promo_id")
w5 = Window.partitionBy("promo_code").orderBy(F.col("promo_id").desc(), "id", "promo_id")
w6 = Window.partitionBy("channel").orderBy("starts_on", "id", "promo_id")

valid = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2024-03-15").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.when(F.col("discount_pct") >= 0.3, "High").when(F.col("discount_pct") >= 0.18, "Medium").otherwise("Low").alias("discount_pct_level"),
        F.coalesce("channel", F.lit("DIRECT")).alias("channel_filled"),
    )
)

metrics = (
    valid
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("event_seq_2", F.row_number().over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)

with_window = (
    metrics
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") <= F.date_add(F.col("pr.ends_on"), 10)), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct"),
    )
)

metrics_promotions = (
    with_window
    .withColumn("next_promo_id", F.lead("promo_id", 2, 0).over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("seq_no_2", F.row_number().over(w6))
)

cleaned = (
    metrics_promotions
    .filter(F.col("promotion_promo_code").isNotNull() & F.col("promo_code").isin(["BUNDLE", "WELCOME5"]))
    .select("*", F.date_trunc("month", "starts_on").alias("starts_on_period"))
)

result = (
    cleaned
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "channel_filled",
        "event_seq_2",
        "promo_id",
        "promotion_promo_code",
        "next_promo_id",
    )
    .orderBy("starts_on", "promotion_promo_code")
)
