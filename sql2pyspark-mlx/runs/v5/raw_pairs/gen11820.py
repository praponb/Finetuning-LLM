from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("monthly_fee", "id")
w2 = Window.orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.orderBy("prev_monthly_fee")
w4 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("end_date", "id").rowsBetween(-5, -1)
w6 = Window.orderBy("seq_no_avg_5_prior", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.orderBy(F.col("seq_no").desc(), "id")

filtered = (
    df_subscriptions
    .filter((F.col("start_date") > F.to_date(F.lit("2023-07-14"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        F.date_add("end_date", 90).alias("end_date_plus_90d"),
    )
)

enriched = (
    filtered
    .withColumn("prev_monthly_fee", F.lag("monthly_fee").over(w_grp))
    .withColumn("next_monthly_fee", F.lead("monthly_fee").over(w_grp))
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("seq_no", F.row_number().over(w2))
)

with_windows = (
    enriched
    .withColumn("prev_monthly_fee_cume_dist", F.cume_dist().over(w3))
    .withColumn("cumulative_next_monthly_fee", F.sum("next_monthly_fee").over(w4))
    .withColumn("seq_no_avg_5_prior", F.avg("seq_no").over(w5))
)

with_windows_subscriptions = (
    with_windows
    .withColumn("prev_monthly_fee_cume_dist_avg_6", F.avg("prev_monthly_fee_cume_dist").over(w6))
    .withColumn("row_seq", F.row_number().over(w7))
)

result = (
    with_windows_subscriptions
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "prev_monthly_fee",
        "next_monthly_fee",
        "prev_monthly_fee_cume_dist",
        "cumulative_next_monthly_fee",
        "prev_monthly_fee_cume_dist_avg_6",
    )
)
