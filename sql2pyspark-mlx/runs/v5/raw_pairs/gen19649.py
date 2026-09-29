from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy("start_date", "id")
w2 = Window.partitionBy("plan").orderBy("event_seq", "id")
w3 = Window.orderBy("event_seq", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_subscriptions
    .filter(F.col("status") == "ACTIVE")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        F.initcap(F.trim("plan")).alias("plan_title"),
    )
)

valid = (
    filtered
    .filter((F.col("start_date") >= F.lit("2023-06-16").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "start_date",
        "status",
        "plan_title",
        F.upper("plan").alias("plan_upper"),
    )
)

with_windows = (
    valid
    .filter(F.col("start_date").between("2023-12-01", "2024-08-14"))
    .select("*", F.row_number().over(w).alias("event_seq"))
)

base = (
    with_windows
    .filter(F.col("start_date") >= F.to_date(F.lit("2023-05-06")))
    .select(
        "id",
        "plan",
        "status",
        "plan_upper",
        "event_seq",
        F.concat_ws("-", "status", "plan").alias("status_plan_key"),
    )
)

enriched = (
    base
    .select(
        "id",
        "plan",
        "plan_upper",
        "event_seq",
        "status_plan_key",
        F.lead("event_seq").over(w2).alias("next_event_seq"),
        F.row_number().over(w2).alias("seq_no"),
        F.avg("event_seq").over(w3).alias("event_seq_avg_3"),
    )
)

result = (
    enriched
    .select(
        "id",
        "plan",
        "plan_upper",
        "event_seq",
        "seq_no",
        "event_seq_avg_3",
        F.when(F.col("plan").rlike("^(basic|enterprise)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("plan_zone"),
    )
    .orderBy(F.col("event_seq_avg_3").desc_nulls_last(), F.col("plan").desc())
)
