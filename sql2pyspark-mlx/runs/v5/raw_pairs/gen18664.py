from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("status").orderBy("end_date", "id")
w_running = Window.partitionBy("plan").orderBy("end_date", "id")

filtered = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-06-19").cast("date")) & F.col("end_date").between("2023-04-01", "2025-07-01"))
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.length("plan").alias("plan_length"),
    )
)

result = (
    filtered
    .select(
        "id",
        "monthly_fee",
        "status",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.row_number().over(w_running).alias("row_seq_2"),
    )
)
