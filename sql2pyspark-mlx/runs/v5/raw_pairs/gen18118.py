from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(-2, Window.currentRow)
w_ordered = Window.partitionBy("promo_code", "channel").orderBy("ends_on", "id")
w_recent = Window.orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("channel")
w7 = Window.orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("promo_code").orderBy(F.col("channel_sum_event_seq").desc_nulls_first(), "id", "promo_id")

metrics = (
    df_promotions
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.lead("discount_pct", 1).over(w).alias("next_discount_pct"),
        F.dense_rank().over(w2).alias("discount_pct_dense_rank"),
        F.avg("discount_pct").over(w3).alias("discount_pct_avg_2"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.sum("discount_pct").over(w_recent).alias("running_discount_pct"),
    )
)

cleaned = (
    metrics
    .filter((F.col("ends_on") > F.to_date(F.lit("2023-06-02"))) & F.col("channel").isNotNull())
    .withColumn("discount_pct_avg_2_times_discount_pct_dense_rank", (F.col("discount_pct_avg_2") * F.col("discount_pct_dense_rank")))
    .withColumn("channel_clean", F.coalesce("channel", F.lit("DIRECT")))
)

scored = (
    cleaned
    .withColumn("channel_sum_event_seq", F.sum("event_seq").over(w6))
    .withColumn("first_known_running_discount_pct", F.first("running_discount_pct", ignorenulls=True).over(w7))
)

matched = (
    scored
    .alias("s")
    .join(df_promotions.alias("pr"), (F.col("s.channel") == F.col("pr.channel")) & (F.col("s.ends_on") >= F.col("pr.starts_on")) & (F.col("s.ends_on") < F.col("pr.ends_on") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "s.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code").alias("promotion_promo_code"),
        F.col("pr.discount_pct").alias("promotion_discount_pct"),
    )
)

ranked = (
    matched
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select(
        "id",
        "promo_id",
        "promo_code",
        "next_discount_pct",
        "discount_pct_avg_2_times_discount_pct_dense_rank",
    )
    .unpivot(
        ids=["id", "promo_id", "promo_code"],
        values=["next_discount_pct", "discount_pct_avg_2_times_discount_pct_dense_rank"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
