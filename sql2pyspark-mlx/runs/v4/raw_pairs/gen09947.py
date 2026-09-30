from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id")
w_grp = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_subscriptions
    .filter(F.col("end_date") >= F.to_date(F.lit("2023-11-08")))
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.when(F.col("monthly_fee") >= 194, "High").when(F.col("monthly_fee") >= 109, "Medium").otherwise("Low").alias("monthly_fee_band"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.sum("monthly_fee").over(w_grp).alias("running_monthly_fee"),
    )
)

cleaned_subscriptions = (
    scored
    .filter(F.col("start_date") > F.to_date(F.lit("2023-09-12")))
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "event_seq",
        "running_monthly_fee",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
    )
)

result = (
    cleaned_subscriptions
    .groupBy(F.date_trunc("MONTH", "end_date").alias("activity_month"))
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("event_seq")).otherwise(0)).alias("enterprise_event_seq"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_rows"),
    )
    .orderBy(F.col("cancelled_rows").desc_nulls_last())
)
