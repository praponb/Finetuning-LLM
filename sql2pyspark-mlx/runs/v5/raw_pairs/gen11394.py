from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("promotion_discount_pct", "id", "promo_id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy("starts_on", "id", "promo_id")
w3 = Window.partitionBy("promotion_promo_code", "channel").orderBy(F.col("promo_id").asc_nulls_last())

valid = (
    df_promotions
    .filter((F.col("ends_on") >= F.to_date(F.lit("2023-11-13"))) & F.col("channel").isNotNull())
    .select("*", F.lower("promo_code").alias("promo_code_lower"))
)

matched = (
    valid
    .alias("e")
    .join(df_promotions.alias("pr"), (F.col("e.channel") == F.col("pr.channel")) & (F.col("e.ends_on") >= F.col("pr.starts_on")) & (F.col("e.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

scored = (
    matched
    .filter(F.col("ends_on") >= "2025-02-12")
    .select(
        "*",
        F.avg("promotion_discount_pct").over(w).alias("promotion_discount_pct_avg_6"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

base = (
    scored
    .filter(F.col("channel").isNotNull() & (F.col("ends_on") >= F.lit("2023-11-08").cast("date")))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "promo_code_lower",
        "promo_id",
        "promotion_promo_code",
        "promotion_discount_pct",
        "promotion_discount_pct_avg_6",
        "row_seq",
        F.upper("promo_code").alias("promo_code_upper"),
        F.coalesce("promotion_promo_code", F.lit("DIRECT")).alias("resolved_promotion_promo_code"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

base_promotions = (
    ranked
    .filter(F.col("channel").isNotNull() & (F.col("row_seq") >= 8))
    .select("*", F.col("row_seq").cast("double").alias("row_seq_num"))
)

result = (
    base_promotions
    .groupBy("promotion_promo_code")
    .pivot("channel", ["partner", "mobile", "store"])
    .agg(F.sum("row_seq").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
