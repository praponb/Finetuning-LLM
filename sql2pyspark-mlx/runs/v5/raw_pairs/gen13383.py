from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("is_welcome5", "id")
w2 = Window.partitionBy("promo_code").orderBy("is_welcome5_rounded", "id")
w3 = Window.partitionBy("channel").orderBy(F.col("ends_on_month_end").desc(), "id")
w_part = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("ends_on_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-07-31").cast("date")) & (F.col("channel").eqNullSafe(F.lit("store"))))
    .select(
        "*",
        F.when(F.col("promo_code") == "WELCOME5", 1).otherwise(0).alias("is_welcome5"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_filled"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

valid = (
    deduped
    .filter((F.col("starts_on") >= F.to_date(F.lit("2023-07-31"))) & F.col("discount_pct").between(0.2, 0.26))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "is_welcome5",
        "channel_filled",
        F.last_day("ends_on").alias("ends_on_month_end"),
        F.round("is_welcome5", 2).alias("is_welcome5_rounded"),
    )
)

with_windows = (
    valid
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("first_known_is_welcome5", F.first("is_welcome5", ignorenulls=True).over(w_part))
    .withColumn("last_known_is_welcome5", F.last("is_welcome5", ignorenulls=True).over(w5))
)

attributed = (
    with_windows
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    attributed
    .select(
        "id",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "discount_pct",
    )
    .unpivot(
        ids=["id", "promo_id", "promotion_promo_code"],
        values=["promotion_discount_pct", "discount_pct"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
