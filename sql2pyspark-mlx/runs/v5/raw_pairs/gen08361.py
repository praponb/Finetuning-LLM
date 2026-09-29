from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("monthly_fee", "id")
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("plan").orderBy(F.col("end_date").desc(), "id")
w_grp = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(-5, -1)
w_running = Window.partitionBy("status")
w6 = Window.partitionBy("status", "plan").orderBy(F.col("next_monthly_fee_avg_5_prior").desc_nulls_last(), "id")
w7 = Window.orderBy("status_max_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status", "plan").orderBy(F.col("seq_no").desc())
w9 = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(-3, Window.currentRow)

with_windows = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.lead("monthly_fee", 1).over(w).alias("next_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "status",
        "next_monthly_fee",
        F.row_number().over(w3).alias("seq_no"),
        F.avg("next_monthly_fee").over(w_grp).alias("next_monthly_fee_avg_5_prior"),
        F.max("monthly_fee").over(w_running).alias("status_max_monthly_fee"),
    )
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics_subscriptions = (
    latest
    .filter(F.col("next_monthly_fee").between(120, 180))
    .select(
        "id",
        "plan",
        "status",
        "next_monthly_fee_avg_5_prior",
        "status_max_monthly_fee",
        F.first("next_monthly_fee_avg_5_prior", ignorenulls=True).over(w7).alias("first_known_next_monthly_fee_avg_5_prior"),
        F.cume_dist().over(w8).alias("seq_no_cume_dist"),
        F.avg("next_monthly_fee_avg_5_prior").over(w9).alias("next_monthly_fee_avg_5_prior_avg_3"),
    )
)

wide = (
    metrics_subscriptions
    .groupBy("status")
    .pivot("plan", ["enterprise", "basic"])
    .agg(F.sum("first_known_next_monthly_fee_avg_5_prior"))
)

result = (
    wide
    .select(
        "status",
        "enterprise",
        "basic",
        F.when(F.col("status").rlike("^(ACTIVE|EXPIRED)$"), "CORE").otherwise("OTHER_ZONE").alias("status_zone"),
    )
)
