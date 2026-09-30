from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id")
w3 = Window.partitionBy("channel")
w4 = Window.partitionBy("channel").orderBy("starts_on", "id")
w5 = Window.partitionBy("promotion_promo_code").orderBy("starts_on", "id", "promo_id", "promo_id_2")
w6 = Window.partitionBy("promotion_promo_code")
w_recent = Window.orderBy("ends_on", "id", "promo_id", "promo_id_2")
w8 = Window.partitionBy("channel").orderBy("row_seq", "id", "promo_id", "promo_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2023-05-31").cast("date")) & F.col("channel").isNotNull())
    .withColumn("days_ends_on_to_starts_on", F.datediff("starts_on", "ends_on"))
    .withColumn("discount_pct_int", F.col("discount_pct").cast("int"))
)

metrics = (
    base
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "days_ends_on_to_starts_on",
        "discount_pct_int",
        F.sum("discount_pct_int").over(w).alias("running_discount_pct_int"),
        F.row_number().over(w2).alias("seq_no"),
        F.min("discount_pct_int").over(w3).alias("channel_min_discount_pct_int"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

with_window = (
    metrics
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel").eqNullSafe(F.col("pr.channel"))) & (F.col("a.ends_on") >= F.col("pr.starts_on")) & (F.col("a.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)

matched = (
    with_window
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & F.col("s.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

enriched = (
    matched
    .select(
        "id",
        "days_ends_on_to_starts_on",
        "discount_pct_int",
        "seq_no",
        "row_seq",
        "promo_id",
        "discount_pct",
        "promo_id_2",
        "promotion_promo_code",
        F.lag("discount_pct", 2).over(w5).alias("prev_discount_pct"),
        F.max("discount_pct_int").over(w6).alias("promotion_promo_code_max_discount_pct_int"),
        F.lag("seq_no", 1).over(w_recent).alias("prev_seq_no"),
        F.first("promo_id", ignorenulls=True).over(w8).alias("first_known_promo_id"),
    )
)

result = (
    enriched
    .select(
        "id",
        "discount_pct_int",
        "row_seq",
        "promo_id",
        "discount_pct",
        "promo_id_2",
        "prev_discount_pct",
        "first_known_promo_id",
    )
)
