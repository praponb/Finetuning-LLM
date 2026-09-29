from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("start_date", "id")
w3 = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w4 = Window.partitionBy("status").orderBy("first_known_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("prev_monthly_fee_scaled").desc())
w6 = Window.orderBy("monthly_fee", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.orderBy("start_date", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("prev_monthly_fee", "id")
w10 = Window.orderBy("monthly_fee", "id").rowsBetween(-3, Window.currentRow)

scored = (
    df_subscriptions
    .select(
        "*",
        F.first("monthly_fee", ignorenulls=True).over(w).alias("first_known_monthly_fee"),
        F.lag("monthly_fee", 2).over(w2).alias("prev_monthly_fee"),
    )
)

filtered = (
    scored
    .filter(F.col("prev_monthly_fee").between(170, 180) & (F.col("start_date") > F.lit("2023-09-05").cast("date")))
    .select(
        "*",
        F.greatest("monthly_fee", "prev_monthly_fee").alias("greatest_monthly_fee_prev_monthly_fee"),
        (F.col("prev_monthly_fee") + F.col("first_known_monthly_fee")).alias("prev_monthly_fee_plus_first_known_monthly_fee"),
    )
)

scored_subscriptions = (
    filtered
    .filter(F.col("start_date") > "2025-05-01")
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "first_known_monthly_fee",
        "prev_monthly_fee",
        "greatest_monthly_fee_prev_monthly_fee",
        "prev_monthly_fee_plus_first_known_monthly_fee",
        F.row_number().over(w3).alias("row_seq"),
        F.last("monthly_fee", ignorenulls=True).over(w4).alias("last_known_monthly_fee"),
    )
)

base = (
    scored_subscriptions
    .filter(F.year("start_date") == 2025)
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "first_known_monthly_fee",
        "prev_monthly_fee",
        "greatest_monthly_fee_prev_monthly_fee",
        "prev_monthly_fee_plus_first_known_monthly_fee",
        "row_seq",
        "last_known_monthly_fee",
        F.round((F.col("prev_monthly_fee") * 100), 2).alias("prev_monthly_fee_scaled"),
        F.date_trunc("week", "start_date").alias("start_date_period"),
    )
)

enriched = (
    base
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "prev_monthly_fee",
        "greatest_monthly_fee_prev_monthly_fee",
        "row_seq",
        "last_known_monthly_fee",
        "prev_monthly_fee_scaled",
        "start_date_period",
        F.rank().over(w5).alias("prev_monthly_fee_scaled_rank"),
        F.avg("row_seq").over(w6).alias("row_seq_avg_6"),
        F.avg("prev_monthly_fee").over(w7).alias("prev_monthly_fee_avg_3"),
        F.first("greatest_monthly_fee_prev_monthly_fee", ignorenulls=True).over(w8).alias("first_known_greatest_monthly_fee_prev_monthly_fee"),
    )
)

scored_subscriptions_2 = (
    enriched
    .withColumn("prev_prev_monthly_fee_avg_3", F.lag("prev_monthly_fee_avg_3").over(w9))
    .withColumn("next_row_seq", F.lead("row_seq").over(w9))
    .withColumn("prev_row_seq", F.lag("row_seq").over(w9))
    .withColumn("row_seq_2", F.row_number().over(w9))
    .withColumn("last_known_monthly_fee_avg_3", F.avg("last_known_monthly_fee").over(w10))
)

result = (
    scored_subscriptions_2
    .withColumn("row_seq_avg_6_tier", F.when(F.col("row_seq_avg_6") >= 8, "LARGE").otherwise("SMALL"))
    .rollup("row_seq_avg_6_tier")
    .agg(
        F.percentile_approx("prev_monthly_fee_scaled_rank", 0.95).alias("prev_monthly_fee_scaled_rank_p95"),
        F.round(F.avg("prev_monthly_fee_scaled"), 2).alias("avg_prev_monthly_fee_scaled"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("prev_monthly_fee_scaled_rank_p95").desc_nulls_first())
)
