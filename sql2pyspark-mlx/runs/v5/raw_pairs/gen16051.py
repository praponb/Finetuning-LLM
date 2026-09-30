from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id")
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status", "order_id").orderBy(F.col("paid_at").desc(), "id")
w4 = Window.partitionBy("method", "order_id").orderBy("is_new_session", "id")
w5 = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(-3, -1)
w6 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w7 = Window.partitionBy("order_id").orderBy("amount_sum_30d", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.orderBy(F.col("is_new_session").asc_nulls_last())
w9 = Window.partitionBy("order_id").orderBy("next_session_id", "id")
w10 = Window.orderBy("is_new_session_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("next_session_id_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy("is_new_session", "id").rowsBetween(-5, Window.currentRow)
w13 = Window.orderBy(F.col("session_id").desc())
w14 = Window.orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)

valid = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-07-08 20:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .select(
        "*",
        F.trim("method").alias("clean_method"),
        F.coalesce("method", F.lit("N/A")).alias("method_clean"),
    )
)

gaps = (
    valid
    .withColumn("is_new_session", F.when(F.lag("paid_at").over(w).isNull() | (F.col("paid_at") > F.lag("paid_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_payments = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

enriched = (
    sessions_payments
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "method_clean",
        "is_new_session",
        "session_id",
        F.row_number().over(w3).alias("seq_no"),
        F.lead("session_id", 1, 0).over(w4).alias("next_session_id"),
        F.avg("session_id").over(w5).alias("session_id_avg_3_prior"),
        F.sum("amount").over(w6).alias("amount_sum_30d"),
    )
)

enriched_payments = (
    enriched
    .filter(F.col("session_id_avg_3_prior") < 5)
    .select(
        "id",
        "paid_at",
        "is_new_session",
        "session_id",
        "seq_no",
        F.avg("next_session_id").over(w7).alias("next_session_id_avg_6"),
        F.percent_rank().over(w8).alias("is_new_session_percent_rank"),
        F.lag("is_new_session", 1).over(w9).alias("prev_is_new_session"),
    )
)

with_windows = (
    enriched_payments
    .select(
        "id",
        "paid_at",
        "is_new_session",
        "next_session_id_avg_6",
        "prev_is_new_session",
        F.sum("is_new_session").over(w10).alias("cumulative_is_new_session"),
        F.sum("prev_is_new_session").over(w11).alias("running_prev_is_new_session"),
        F.avg("is_new_session").over(w12).alias("is_new_session_avg_5"),
        F.rank().over(w13).alias("session_id_rank"),
        F.max("prev_is_new_session").over(w14).alias("prev_is_new_session_max_7d"),
    )
)

daily = (
    with_windows
    .withColumn("year_start", F.date_trunc("YEAR", "paid_at"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "paid_at"))
    .groupingSets(
        [["year_start", "quarter_start"], ["year_start"], []],
        "year_start",
        "quarter_start",
    )
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.percentile_approx("is_new_session_avg_5", 0.9).alias("is_new_session_avg_5_p90"),
        F.sum("next_session_id_avg_6").alias("total_next_session_id_avg_6"),
        F.min("paid_at").alias("first_paid_at_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("year_start").alias("grouping_id"),
    )
)

result = (
    daily
    .select(
        "year_start",
        "quarter_start",
        "first_paid_at",
        "is_new_session_avg_5_p90",
        "first_paid_at_2",
        "distinct_ids",
        "grouping_id",
    )
    .orderBy(F.col("is_new_session_avg_5_p90").desc_nulls_last())
)
