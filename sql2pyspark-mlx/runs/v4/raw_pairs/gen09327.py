from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_id").orderBy("salary", "id")
w3 = Window.partitionBy("manager_id", "level")
w_recent = Window.partitionBy("job_title", "dept_id").orderBy(F.col("salary").desc())
w5 = Window.partitionBy("level").orderBy("salary_rank", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy(F.col("cumulative_salary").desc_nulls_first())
w7 = Window.partitionBy("level").orderBy(F.col("cumulative_salary_cume_dist").desc())
w8 = Window.partitionBy("level").orderBy("cumulative_salary_cume_dist", "id").rowsBetween(-5, -1)

valid = (
    df_employees
    .filter(F.col("hire_date") >= F.to_date(F.lit("2023-12-01")))
    .select(
        "*",
        F.coalesce("job_title", F.lit("Unknown")).alias("job_title_filled"),
        F.coalesce("level", F.lit("UNKNOWN")).alias("level_filled"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.sum("salary").over(w).alias("cumulative_salary"),
        F.row_number().over(w2).alias("row_seq"),
        F.round((F.col("salary") / F.sum("salary").over(w3)), 4).alias("salary_share"),
        F.rank().over(w_recent).alias("salary_rank"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.avg("salary_share").over(w5).alias("salary_share_avg_6"),
        F.cume_dist().over(w6).alias("cumulative_salary_cume_dist"),
    )
)

with_windows_employees = (
    metrics
    .select(
        "*",
        F.cume_dist().over(w7).alias("cumulative_salary_cume_dist_cume_dist"),
        F.avg("salary_share_avg_6").over(w8).alias("salary_share_avg_6_avg_5_prior"),
    )
)

result = (
    with_windows_employees
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "level",
        "job_title_filled",
    )
    .orderBy(F.col("manager_id").asc_nulls_last())
)
