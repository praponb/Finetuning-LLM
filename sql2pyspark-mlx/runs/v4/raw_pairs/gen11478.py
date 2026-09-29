from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("discount_pct", "id")
w2 = Window.partitionBy("channel").orderBy(F.col("starts_on_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy(F.col("row_seq").desc(), "id")
w4 = Window.partitionBy("channel").orderBy("starts_on_period", "id", "promo_id")
w5 = Window.partitionBy("channel").orderBy("starts_on_period", "id", "promo_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2023-08-30").cast("date")) & F.col("promo_code").isin(["BUNDLE", "SPRING10"]))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.when(F.col("discount_pct") >= 0.3, "High").when(F.col("discount_pct") >= 0.18, "Medium").otherwise("Low").alias("discount_pct_tier"),
        F.date_trunc("month", "starts_on").alias("starts_on_period"),
    )
)

metrics = (
    base
    .select(
        "*",
        F.row_number().over(w_recent).alias("row_seq"),
        F.max("discount_pct").over(w2).alias("discount_pct_max_7d"),
    )
)

base_promotions = (
    metrics
    .filter(F.col("channel").isNotNull() & (F.to_date("starts_on_period") >= "2025-01-01"))
    .withColumn("days_since_starts_on", F.datediff(F.current_date(), "starts_on"))
)

latest = (
    base_promotions
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

matched = (
    latest
    .alias("a")
    .join(df_promotions.alias("pr"), (F.col("a.channel") == F.col("pr.channel")) & F.col("a.ends_on").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "a.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

flagged = (
    matched
    .withColumn("is_new_session", F.when(F.col("starts_on_period") - F.lag("starts_on_period").over(w4) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_promotions = flagged.withColumn("burst_id", F.sum("is_new_session").over(w5))

result = (
    sessions_promotions
    .withColumn("is_new_session_tier", F.when(F.col("is_new_session") >= 1, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["channel", "is_new_session_tier"], ["channel"], []],
        "channel",
        "is_new_session_tier",
    )
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", F.col("promotion_discount_pct")).otherwise(0)).alias("mobile_promotion_discount_pct"),
        F.sort_array(F.collect_set("promo_code")).alias("promo_code_list"),
        F.avg("row_seq").alias("avg_row_seq"),
        F.grouping_id().alias("grouping_level"),
    )
)
