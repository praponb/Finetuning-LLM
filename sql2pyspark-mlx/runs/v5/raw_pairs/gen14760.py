from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("plan", "status").orderBy("customer_id_sum_monthly_fee", "id")
w4 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy(F.col("prev_first_known_monthly_fee").desc(), "id")
w_part = Window.partitionBy("plan")
w_grp_2 = Window.partitionBy("status").orderBy("customer_id_sum_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_subscriptions
    .filter(F.col("plan") == "premium")
    .select(
        "id",
        "plan",
        "start_date",
        "status",
        F.first("monthly_fee", ignorenulls=True).over(w).alias("first_known_monthly_fee"),
        F.sum("monthly_fee").over(w2).alias("customer_id_sum_monthly_fee"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.lag("customer_id_sum_monthly_fee").over(w3).alias("prev_customer_id_sum_monthly_fee"),
        F.lead("customer_id_sum_monthly_fee").over(w3).alias("next_customer_id_sum_monthly_fee"),
        F.lag("first_known_monthly_fee").over(w3).alias("prev_first_known_monthly_fee"),
        F.lead("first_known_monthly_fee").over(w3).alias("next_first_known_monthly_fee"),
        F.row_number().over(w3).alias("event_seq"),
        F.first("customer_id_sum_monthly_fee", ignorenulls=True).over(w4).alias("first_known_customer_id_sum_monthly_fee"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "plan",
        "status",
        "first_known_monthly_fee",
        "prev_customer_id_sum_monthly_fee",
        "next_customer_id_sum_monthly_fee",
        "prev_first_known_monthly_fee",
        "event_seq",
        "first_known_customer_id_sum_monthly_fee",
        F.ntile(5).over(w_grp).alias("prev_first_known_monthly_fee_bucket"),
        F.stddev_samp("first_known_monthly_fee").over(w_part).alias("plan_stddev_samp_first_known_monthly_fee"),
        F.first("customer_id_sum_monthly_fee", ignorenulls=True).over(w_grp_2).alias("first_known_customer_id_sum_monthly_fee_2"),
    )
)

valid = (
    with_windows
    .filter(F.col("event_seq") <= 3)
    .select(
        "id",
        "plan",
        "prev_customer_id_sum_monthly_fee",
        "next_customer_id_sum_monthly_fee",
        "prev_first_known_monthly_fee",
        "event_seq",
        "first_known_customer_id_sum_monthly_fee",
        "first_known_customer_id_sum_monthly_fee_2",
        F.floor("first_known_monthly_fee").alias("first_known_monthly_fee_whole"),
        F.greatest("plan_stddev_samp_first_known_monthly_fee", "event_seq").alias("greatest_plan_stddev_samp_first_known_monthly_fee_event_seq"),
    )
)

result = (
    valid
    .cube(
        F.when(F.col("first_known_monthly_fee_whole") >= 69, "HIGH").otherwise("LOW").alias("first_known_monthly_fee_whole_tier"),
        "plan",
    )
    .agg(
        F.sum("prev_customer_id_sum_monthly_fee").alias("total_prev_customer_id_sum_monthly_fee"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
    )
)
