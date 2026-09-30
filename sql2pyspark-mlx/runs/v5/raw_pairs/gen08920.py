from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("hire_date", "id")
w_recent = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("manager_id").orderBy("salary_whole", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("job_title", "level").orderBy(F.col("row_seq").desc_nulls_first(), "id")
w5 = Window.partitionBy("level").orderBy("salary_avg_5", "id")
w6 = Window.partitionBy("job_title").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-02-18"))) & (F.col("hire_date") < "2025-02-01"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.ceil("salary").alias("salary_whole"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "full_name",
        "job_title",
        "salary",
        "level",
        F.row_number().over(w_grp).alias("row_seq"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.avg("salary").over(w3).alias("salary_avg_5"),
    )
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

scored = (
    latest
    .select(
        "id",
        "job_title",
        "level",
        "row_seq",
        "event_seq",
        F.lag("salary").over(w5).alias("prev_salary"),
        F.lead("salary").over(w5).alias("next_salary"),
        F.lag("salary_avg_5").over(w5).alias("prev_salary_avg_5"),
        F.lead("salary_avg_5").over(w5).alias("next_salary_avg_5"),
        F.row_number().over(w5).alias("row_seq_2"),
        F.first("event_seq", ignorenulls=True).over(w6).alias("first_known_event_seq"),
    )
)

result = (
    scored
    .groupBy("level")
    .agg(
        F.count("*").alias("event_count"),
        F.size(F.collect_set("level")).alias("num_levels"),
    )
    .orderBy(F.col("event_count").asc_nulls_last())
)
