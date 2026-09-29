from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promotion_promo_code_2").orderBy("ends_on_period", "id", "promo_id", "promo_id_2")
w2 = Window.partitionBy("promotion_promo_code_2").orderBy("ends_on_period", "id", "promo_id", "promo_id_2").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "*",
        F.last_day("ends_on").alias("ends_on_month_end"),
        F.date_trunc("WEEK", "ends_on").alias("ends_on_period"),
        F.coalesce("channel", F.lit("N/A")).alias("channel_clean"),
    )
)

with_window = (
    base
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.ends_on_month_end").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "left")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

matched = (
    with_window
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.starts_on") >= F.col("pr.starts_on")) & (F.col("e.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id_2"),
        F.col("pr.promo_code").alias("promotion_promo_code_2"),
        F.col("pr.discount_pct").alias("promotion_discount_pct_2"),
    )
)

gaps = (
    matched
    .withColumn("new_group_flag", F.when(F.lag("ends_on_period").over(w).isNull() | (F.col("ends_on_period") > F.lag("ends_on_period").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

with_gaps = (
    islands
    .withColumn("is_new_session", F.when(F.lag("ends_on_period").over(w).isNull() | (F.col("ends_on_period") > F.lag("ends_on_period").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("QUARTER", "ends_on_period").alias("quarter_start"))
    .agg(
        F.max("ends_on_period").alias("last_seen"),
        F.min("ends_on_period").alias("first_ends_on_period"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
    )
    .filter(F.col("partner_rows") < 1000)
    .orderBy("partner_rows")
)
