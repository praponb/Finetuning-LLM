from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("monthly_fee").desc())
w_ordered = Window.partitionBy("plan").orderBy(F.col("start_date").desc(), "id")
w3 = Window.orderBy("start_date", "id")
w4 = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("plan").orderBy("end_date", "id")
w6 = Window.partitionBy("customer_id").orderBy("days_start_date_to_end_date", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.partitionBy("plan", "status").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status")
w9 = Window.partitionBy("plan").orderBy("days_start_date_to_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_subscriptions
    .select(
        "*",
        F.cume_dist().over(w).alias("monthly_fee_cume_dist"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.lead("monthly_fee_cume_dist", 1, 0).over(w3).alias("next_monthly_fee_cume_dist"),
        F.first("seq_no", ignorenulls=True).over(w4).alias("first_known_seq_no"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

valid = (
    with_windows
    .filter(F.col("end_date") > F.lit("2023-10-07").cast("date"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        "seq_no",
        "next_monthly_fee_cume_dist",
        "event_seq",
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)

result = (
    valid
    .select(
        "*",
        F.avg("next_monthly_fee_cume_dist").over(w6).alias("next_monthly_fee_cume_dist_avg_2"),
        F.sum("next_monthly_fee_cume_dist").over(w_part).alias("running_next_monthly_fee_cume_dist"),
        F.round((F.col("event_seq") / F.sum("event_seq").over(w8)), 4).alias("event_seq_share"),
        F.first("next_monthly_fee_cume_dist", ignorenulls=True).over(w9).alias("first_known_next_monthly_fee_cume_dist"),
    )
)
