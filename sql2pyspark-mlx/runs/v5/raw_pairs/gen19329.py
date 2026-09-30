from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("days_since_start_date", "id")
w2 = Window.partitionBy("status")
w3 = Window.orderBy("days_end_date_to_start_date", "id")
w4 = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country", "plan").orderBy("seq_no", "id")
w7 = Window.partitionBy("plan")
w_ordered = Window.orderBy("start_date", "id")

valid = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-05-24").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.datediff("start_date", "end_date").alias("days_end_date_to_start_date"),
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
    )
)

enriched = (
    valid
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.name"), F.col("p.country"), F.col("p.age"))
)

metrics = (
    enriched
    .filter(F.col("plan") == "premium")
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.avg("days_end_date_to_start_date").over(w2).alias("status_avg_days_end_date_to_start_date"),
        F.lag("age", 1, 0).over(w3).alias("prev_age"),
        F.first("monthly_fee", ignorenulls=True).over(w4).alias("first_known_monthly_fee"),
        F.sum("monthly_fee").over(w5).alias("running_monthly_fee"),
    )
)

enriched_subscriptions = (
    metrics
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "days_end_date_to_start_date",
        "country",
        "age",
        "prev_age",
        "running_monthly_fee",
        F.lead("age").over(w6).alias("next_age"),
        F.lag("first_known_monthly_fee").over(w6).alias("prev_first_known_monthly_fee"),
        F.row_number().over(w6).alias("row_seq"),
    )
)

result = (
    enriched_subscriptions
    .select(
        "id",
        "start_date",
        "days_end_date_to_start_date",
        "age",
        "prev_age",
        "running_monthly_fee",
        F.min("days_end_date_to_start_date").over(w7).alias("plan_min_days_end_date_to_start_date"),
        F.lead("running_monthly_fee", 1).over(w_ordered).alias("next_running_monthly_fee"),
    )
)
