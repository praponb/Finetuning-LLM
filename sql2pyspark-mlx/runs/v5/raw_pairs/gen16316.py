from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary_whole", "id")
w2 = Window.partitionBy("job_title", "level").orderBy("hire_date", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("salary_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("salary").desc(), "id")
w_running = Window.orderBy("salary_avg_2", "id").rowsBetween(-5, Window.currentRow)
w_recent = Window.partitionBy("level").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("last_known_salary_avg_2", "id")
w8 = Window.partitionBy("dept_id").orderBy("cumulative_is_l5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("last_known_salary_avg_2_whole", "id")
w10 = Window.orderBy("cumulative_salary_whole_avg_5", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("is_l5", "id")
w12 = Window.orderBy(F.col("salary_avg_2").desc(), "id")
w13 = Window.orderBy(F.col("is_l5").asc_nulls_last())

base = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (F.col("manager_id") == 35))
    .select(
        "id",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.concat_ws(",", "level", "job_title").alias("level_job_title_key"),
        F.ceil("salary").alias("salary_whole"),
    )
)

valid = (
    base
    .filter(F.year("hire_date") == 2024)
    .withColumn("is_l5", F.when(F.col("level") == "L5", 1).otherwise(0))
)

with_windows = (
    valid
    .select(
        "*",
        F.lag("salary_whole", 1, 0).over(w).alias("prev_salary_whole"),
        F.avg("salary").over(w2).alias("salary_avg_2"),
        F.sum("is_l5").over(w3).alias("cumulative_is_l5"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "email",
        "dept_id",
        "salary",
        "is_remote",
        "level_job_title_key",
        "salary_whole",
        "is_l5",
        "prev_salary_whole",
        "salary_avg_2",
        "cumulative_is_l5",
        F.avg("salary_whole").over(w_running).alias("salary_whole_avg_5"),
        F.last("salary_avg_2", ignorenulls=True).over(w_recent).alias("last_known_salary_avg_2"),
    )
)

base_employees = (
    scored
    .filter(F.upper(F.trim("email")) == "IVY@GMAIL.COM")
    .select(
        "*",
        F.floor("last_known_salary_avg_2").alias("last_known_salary_avg_2_whole"),
        F.round((F.col("last_known_salary_avg_2") / F.col("salary_avg_2")), 2).alias("last_known_salary_avg_2_per_salary_avg_2"),
    )
)

enriched = (
    base_employees
    .select(
        "id",
        "salary",
        "level_job_title_key",
        "salary_whole",
        "is_l5",
        "salary_avg_2",
        "salary_whole_avg_5",
        "last_known_salary_avg_2_whole",
        "last_known_salary_avg_2_per_salary_avg_2",
        F.row_number().over(w7).alias("event_seq"),
        F.sum("salary_whole_avg_5").over(w8).alias("cumulative_salary_whole_avg_5"),
    )
)

enriched_employees = (
    enriched
    .withColumn("last_known_salary_avg_2_whole_bucket", F.ntile(5).over(w9))
    .withColumn("last_known_event_seq", F.last("event_seq", ignorenulls=True).over(w10))
)

result = (
    enriched_employees
    .filter(F.col("last_known_salary_avg_2_whole_bucket").between(3, 5))
    .withColumn("row_seq", F.row_number().over(w11))
    .withColumn("salary_avg_2_bucket", F.ntile(10).over(w12))
    .withColumn("is_l5_rank", F.rank().over(w13))
)
