from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id")
w2 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("manager_id", "dept_id").orderBy("salary", "id")
w4 = Window.orderBy("prev_salary", "id")
w_recent = Window.orderBy("hire_date", "id")
w_part = Window.orderBy("prev_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("event_seq", "id")
w_grp = Window.orderBy("next_prev_salary", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.orderBy("event_seq_bucket", "id").rowsBetween(-2, Window.currentRow)
w10 = Window.orderBy(F.col("event_seq_bucket").asc_nulls_last())
w11 = Window.orderBy(F.col("running_event_seq").desc(), "id")
w12 = Window.orderBy(F.col("running_event_seq_2").asc_nulls_last())
w13 = Window.orderBy(F.col("last7_count_running_event_seq").desc(), "id")
w14 = Window.orderBy(F.col("event_seq_bucket").desc(), "id")
w15 = Window.orderBy("last7_avg_running_event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w16 = Window.orderBy("running_event_seq_bucket", "id")

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-05-18").cast("date")) & F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.concat("level", F.lit(", "), "email").alias("level_email"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_clean"),
    )
)

enriched = (
    base
    .filter(F.month("hire_date").isin([6, 9, 10]))
    .select(
        "id",
        "full_name",
        "email",
        "hire_date",
        "is_remote",
        "job_title_clean",
        F.lag("salary", 1).over(w).alias("prev_salary"),
        F.first("level", ignorenulls=True).over(w2).alias("first_known_level"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

enriched_employees = (
    enriched
    .select(
        "id",
        "full_name",
        "email",
        "hire_date",
        "is_remote",
        "job_title_clean",
        F.lead("event_seq", 1).over(w4).alias("next_event_seq"),
        F.lead("prev_salary", 1).over(w_recent).alias("next_prev_salary"),
        F.sum("event_seq").over(w_part).alias("running_event_seq"),
        F.sum("event_seq").over(w_part).alias("running_event_seq_2"),
        F.ntile(10).over(w7).alias("event_seq_bucket"),
    )
)

valid = (
    enriched_employees
    .filter((F.col("hire_date") >= F.lit("2023-03-29").cast("date")) & (F.col("hire_date") <= "2023-04-01"))
    .withColumn("hire_date_month_end", F.last_day("hire_date"))
    .withColumn("running_event_seq_2_whole", F.floor("running_event_seq_2"))
)

metrics = (
    valid
    .select(
        "id",
        "hire_date",
        "job_title_clean",
        "running_event_seq",
        "running_event_seq_2",
        "event_seq_bucket",
        "running_event_seq_2_whole",
        F.max("running_event_seq").over(w_grp).alias("last7_max_running_event_seq"),
        F.avg("running_event_seq").over(w_grp).alias("last7_avg_running_event_seq"),
        F.count("running_event_seq").over(w_grp).alias("last7_count_running_event_seq"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.avg("last7_avg_running_event_seq").over(w9).alias("last7_avg_running_event_seq_avg_2"),
        F.cume_dist().over(w10).alias("event_seq_bucket_cume_dist"),
        F.ntile(5).over(w11).alias("running_event_seq_bucket"),
        F.rank().over(w12).alias("running_event_seq_2_rank"),
        F.ntile(10).over(w13).alias("last7_count_running_event_seq_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "hire_date",
        "job_title_clean",
        "running_event_seq",
        "event_seq_bucket",
        "running_event_seq_2_whole",
        "last7_count_running_event_seq",
        "last7_avg_running_event_seq_avg_2",
        F.ntile(5).over(w14).alias("event_seq_bucket_bucket"),
        F.last("last7_avg_running_event_seq", ignorenulls=True).over(w15).alias("last_known_last7_avg_running_event_seq"),
        F.row_number().over(w11).alias("event_seq"),
        F.row_number().over(w16).alias("row_seq"),
    )
)
