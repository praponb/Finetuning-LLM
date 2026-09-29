from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id").orderBy("end_date", "id")
w_grp = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy(F.col("next_adjusted_monthly_fee").desc(), "id")
w_running = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_subscriptions
    .filter((F.col("end_date") > F.to_date(F.lit("2023-09-16"))) & F.col("start_date").between("2023-07-02", "2023-12-26"))
    .withColumn("adjusted_monthly_fee", F.round((F.col("monthly_fee") * 1.07), 2))
)

enriched = (
    filtered
    .select(
        "id",
        "customer_id",
        "start_date",
        "status",
        F.lag("monthly_fee").over(w_part).alias("prev_monthly_fee"),
        F.lead("adjusted_monthly_fee").over(w_part).alias("next_adjusted_monthly_fee"),
        F.lag("adjusted_monthly_fee").over(w_part).alias("prev_adjusted_monthly_fee"),
        F.row_number().over(w_part).alias("seq_no"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "start_date", "next_adjusted_monthly_fee", "seq_no")
)

with_windows = (
    latest
    .select(
        "id",
        "next_adjusted_monthly_fee",
        "seq_no",
        F.max("seq_no").over(w_running).alias("running_max_seq_no"),
        F.count("seq_no").over(w_running).alias("running_count_seq_no"),
        F.sum("seq_no").over(w_running).alias("running_sum_seq_no"),
    )
)

base = (
    with_windows
    .filter(F.col("running_sum_seq_no").between(18, 24))
    .select(
        "id",
        "next_adjusted_monthly_fee",
        "seq_no",
        "running_max_seq_no",
        "running_count_seq_no",
        "running_sum_seq_no",
        F.col("seq_no").cast("string").alias("seq_no_str"),
    )
)

result = (
    base
    .select(
        "id",
        "next_adjusted_monthly_fee",
        "seq_no",
        "running_sum_seq_no",
        "seq_no_str",
    )
    .orderBy(F.col("next_adjusted_monthly_fee").desc())
)
