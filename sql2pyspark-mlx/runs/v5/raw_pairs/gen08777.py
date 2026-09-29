from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code").orderBy("promotion_discount_pct_3", "id", "promo_id", "promo_id_2", "promo_id_3")
w2 = Window.orderBy(F.col("days_ends_on_to_starts_on").desc(), "id", "promo_id", "promo_id_2", "promo_id_3")
w3 = Window.partitionBy("promotion_promo_code", "promotion_promo_code_2")
w4 = Window.orderBy("promo_id_3", "id", "promo_id", "promo_id_2", "promo_id_3").rowsBetween(-3, -1)
w5 = Window.partitionBy("promotion_promo_code_2").orderBy("promotion_promo_code_avg_promotion_discount_pct_3", "id", "promo_id", "promo_id_2", "promo_id_3")
w6 = Window.orderBy(F.col("promo_id").desc(), "id", "promo_id", "promo_id_2", "promo_id_3")
w7 = Window.partitionBy("promotion_promo_code_3", "channel").orderBy("discount_pct", "id", "promo_id", "promo_id_2", "promo_id_3").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .withColumn("days_ends_on_to_starts_on", F.datediff("starts_on", "ends_on"))
    .withColumn("promo_code_length", F.length("promo_code"))
    .withColumn("channel_filled", F.coalesce("channel", F.lit("OTHER")))
)

with_window = (
    filtered
    .alias("src")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("src.channel"), F.lit("store")) == F.col("pr.channel")) & (F.col("src.starts_on") >= F.col("pr.starts_on")) & (F.col("src.starts_on") <= F.date_add(F.col("pr.ends_on"), 3)), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

attributed = (
    with_window
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

matched = (
    attributed
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & F.col("a.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id_3"),
        F.col("pr.promo_code").alias("promotion_promo_code_3"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_3"),
    )
)

with_windows = (
    matched
    .withColumn("promotion_discount_pct_3_bucket", F.ntile(4).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("promotion_promo_code_avg_promotion_discount_pct_3", F.avg("promotion_discount_pct_3").over(w3))
)

with_windows_promotions = (
    with_windows
    .select(
        "*",
        F.avg("promo_code_length").over(w4).alias("promo_code_length_avg_3_prior"),
        F.lag("promotion_discount_pct", 1).over(w5).alias("prev_promotion_discount_pct"),
        F.ntile(3).over(w6).alias("promo_id_bucket"),
        F.first("promo_id_2", ignorenulls=True).over(w7).alias("first_known_promo_id_2"),
    )
)

attributed_promotions = (
    with_windows_promotions
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") <= F.date_add(F.col("pr.ends_on"), 7)), "inner")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_4"),
        F.col("pr.promo_code").alias("promotion_promo_code_4"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_4"),
    )
)

result = (
    attributed_promotions
    .select(
        "id",
        "promo_id",
        "promo_id_2",
        "promo_id_3",
        "promo_id_4",
        "promotion_promo_code_3",
        "promotion_discount_pct_2",
        "prev_promotion_discount_pct",
    )
    .unpivot(
        ids=["id", "promo_id", "promo_id_2", "promo_id_3", "promo_id_4", "promotion_promo_code_3"],
        values=["promotion_discount_pct_2", "prev_promotion_discount_pct"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
