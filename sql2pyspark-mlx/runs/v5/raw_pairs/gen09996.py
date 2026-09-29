from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("plan", "status").orderBy("start_date", "id")
w2 = Window.orderBy("monthly_fee", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("plan").orderBy("running_credit_limit", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(-5, Window.currentRow)
w_recent_2 = Window.partitionBy("plan").orderBy("monthly_fee_avg_2", "id").rowsBetween(-6, -1)
w_part = Window.orderBy("next_credit_limit", "id")
w9 = Window.orderBy(F.col("monthly_fee_avg_2").desc_nulls_last())
w10 = Window.orderBy("next_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy("cumulative_monthly_fee_avg_2", "id")
w13 = Window.orderBy(F.col("cumulative_monthly_fee_avg_2").desc_nulls_last())
w14 = Window.orderBy("next_credit_limit_avg_5", "id").rowsBetween(-6, Window.currentRow)
w15 = Window.orderBy("running_credit_limit_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w16 = Window.orderBy("running_credit_limit", "id")
w17 = Window.orderBy(F.col("running_credit_limit").desc(), "id")
w18 = Window.orderBy(F.col("monthly_fee_avg_2_dense_rank").asc_nulls_last())

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-08-22").cast("date")) & (F.col("end_date") < "2025-06-16"))
    .withColumn("status_plan_key", F.concat_ws("-", "status", "plan"))
)

joined = (
    cleaned
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .select("a.*", F.col("c.name"), F.col("c.credit_limit"))
)

scored = (
    joined
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "status_plan_key",
        "name",
        "credit_limit",
        F.lead("credit_limit", 2).over(w_recent).alias("next_credit_limit"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_2"),
        F.last("credit_limit", ignorenulls=True).over(w3).alias("last_known_credit_limit"),
        F.sum("credit_limit").over(w4).alias("running_credit_limit"),
    )
)

with_windows = (
    scored
    .filter(F.col("running_credit_limit").between(42502, 78065))
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "credit_limit",
        "monthly_fee_avg_2",
        "running_credit_limit",
        F.avg("running_credit_limit").over(w_grp).alias("running_credit_limit_avg_6_prior"),
        F.avg("next_credit_limit").over(w6).alias("next_credit_limit_avg_5"),
        F.sum("running_credit_limit").over(w3).alias("running_running_credit_limit"),
        F.avg("running_credit_limit").over(w_recent_2).alias("running_credit_limit_avg_6_prior_2"),
        F.lead("monthly_fee", 1, 0).over(w_part).alias("next_monthly_fee"),
    )
)

with_windows_subscriptions = (
    with_windows
    .select(
        "id",
        "running_credit_limit",
        "running_credit_limit_avg_6_prior",
        "next_credit_limit_avg_5",
        "running_running_credit_limit",
        F.dense_rank().over(w9).alias("monthly_fee_avg_2_dense_rank"),
        F.sum("running_credit_limit").over(w10).alias("running_running_credit_limit_2"),
        F.sum("monthly_fee_avg_2").over(w11).alias("cumulative_monthly_fee_avg_2"),
    )
)

with_windows_subscriptions_2 = (
    with_windows_subscriptions
    .select(
        "*",
        F.lead("running_running_credit_limit_2", 1, 0).over(w12).alias("next_running_running_credit_limit_2"),
        F.percent_rank().over(w13).alias("cumulative_monthly_fee_avg_2_percent_rank"),
        F.avg("next_credit_limit_avg_5").over(w14).alias("next_credit_limit_avg_5_avg_6"),
        F.sum("running_running_credit_limit_2").over(w15).alias("cumulative_running_running_credit_limit_2"),
    )
)

result = (
    with_windows_subscriptions_2
    .select(
        "id",
        "running_credit_limit",
        "running_credit_limit_avg_6_prior",
        "next_credit_limit_avg_5",
        "running_running_credit_limit",
        "monthly_fee_avg_2_dense_rank",
        "cumulative_monthly_fee_avg_2",
        "next_credit_limit_avg_5_avg_6",
        "cumulative_running_running_credit_limit_2",
        F.lead("next_running_running_credit_limit_2", 1).over(w16).alias("next_next_running_running_credit_limit_2"),
        F.row_number().over(w17).alias("row_seq"),
        F.cume_dist().over(w18).alias("monthly_fee_avg_2_dense_rank_cume_dist"),
        F.lag("cumulative_monthly_fee_avg_2_percent_rank", 1).over(w12).alias("prev_cumulative_monthly_fee_avg_2_percent_rank"),
    )
)
