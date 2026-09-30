from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("promo_code")
w3 = Window.orderBy("promo_id", "id", "promo_id")
w4 = Window.orderBy("ends_on_quarter", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_promotions
    .filter(F.col("starts_on") > F.lit("2023-08-19").cast("date"))
    .select(
        "*",
        F.concat("channel", F.lit(" "), "promo_code").alias("channel_promo_code"),
    )
)

base = (
    cleaned
    .filter((F.col("ends_on") >= F.lit("2024-03-06").cast("date")) & F.col("channel").isNotNull() & (F.col("discount_pct") <= 0.27))
    .withColumn("days_since_ends_on", F.datediff(F.current_date(), "ends_on"))
    .withColumn("ends_on_quarter", F.quarter("ends_on"))
)

metrics = (
    base
    .select(
        "*",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4).alias("discount_pct_share"),
        F.min("days_since_ends_on").over(w2).alias("promo_code_min_days_since_ends_on"),
    )
)

matched = (
    metrics
    .alias("a")
    .join(df_promotions.alias("pr"), (F.coalesce(F.col("a.channel"), F.lit("web")) == F.col("pr.channel")) & (F.col("a.starts_on") >= F.col("pr.starts_on")) & (F.col("a.starts_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

result = (
    matched
    .select(
        "*",
        F.ntile(4).over(w3).alias("promo_id_bucket"),
        F.first("promo_id", ignorenulls=True).over(w4).alias("first_known_promo_id"),
    )
)
