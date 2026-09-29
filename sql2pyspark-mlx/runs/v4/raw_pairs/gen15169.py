from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_first())
w_recent = Window.partitionBy("plan", "status").orderBy(F.col("end_date").desc(), "id")
w_hist = Window.partitionBy("status")

metrics = (
    df_subscriptions
    .select(
        "*",
        F.rank().over(w).alias("monthly_fee_rank"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.count("monthly_fee").over(w_hist).alias("status_count_monthly_fee"),
    )
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "monthly_fee_rank",
        "seq_no",
    )
    .orderBy(F.col("id").desc(), F.col("end_date").desc())
    .limit(5)
)
