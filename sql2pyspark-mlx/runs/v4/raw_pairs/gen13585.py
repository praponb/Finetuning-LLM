from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w2 = Window.partitionBy("channel").orderBy("ends_on", "id")
w3 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("channel").orderBy(F.col("starts_on").desc(), "id")
w5 = Window.orderBy(F.col("discount_pct").desc(), "id")

base = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-04-25").cast("date")) & F.col("channel").isNotNull())
    .withColumn("channel_lower", F.lower("channel"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("OTHER")))
)

with_windows = (
    base
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.lead("discount_pct", 1).over(w2).alias("next_discount_pct"),
        F.sum("discount_pct").over(w3).alias("running_discount_pct"),
        F.row_number().over(w4).alias("row_seq"),
        F.ntile(3).over(w5).alias("discount_pct_bucket"),
    )
)

with_window = (
    with_windows
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") <= F.date_add(F.col("pr.ends_on"), 3)), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    with_window
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "channel_lower",
        "resolved_channel",
        "seq_no",
        "next_discount_pct",
    )
)
