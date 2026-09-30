from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("customer_id", "status").orderBy(F.col("plan_length").desc(), "id")
w2 = Window.orderBy(F.col("plan_length").desc(), "id")
w_hist = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan")
w6 = Window.partitionBy("status", "customer_id").orderBy("monthly_fee", "id")
w7 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("start_date") < "2024-12-12"))
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.length("plan").alias("plan_length"),
    )
)

scored = (
    valid
    .filter(F.col("customer_id") == 15)
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.row_number().over(w_running).alias("event_seq"),
        F.row_number().over(w2).alias("event_seq_2"),
        F.avg("monthly_fee").over(w_hist).alias("monthly_fee_avg_6"),
        F.first("monthly_fee", ignorenulls=True).over(w4).alias("first_known_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w5)), 4).alias("monthly_fee_share"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.lag("event_seq", 2).over(w6).alias("prev_event_seq"),
        F.sum("monthly_fee").over(w7).alias("running_monthly_fee"),
    )
)

filtered = (
    with_windows
    .filter(F.col("end_date") > F.to_date(F.lit("2023-08-16")))
    .withColumn("plan_length", F.length("plan"))
)

result = (
    filtered
    .groupBy("plan", "customer_id")
    .pivot("status", ["EXPIRED", "CANCELLED"])
    .agg(F.avg("first_known_monthly_fee"))
    .select(
        "plan",
        "customer_id",
        F.col("EXPIRED").alias("expired"),
        F.col("CANCELLED").alias("cancelled"),
    )
)
