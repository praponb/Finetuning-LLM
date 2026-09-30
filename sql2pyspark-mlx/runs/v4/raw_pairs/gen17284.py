from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id")
w2 = Window.partitionBy("dept_id").orderBy("salary", "id")
w_recent = Window.partitionBy("job_title").orderBy("prev_salary_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("prev_salary_2", "id").rowsBetween(-2, Window.currentRow)
w_grp = Window.partitionBy("job_title").orderBy(F.col("prev_salary").desc(), "id")
w6 = Window.partitionBy("dept_id").orderBy("prev_salary_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.min("salary").alias("min_employee_salary"))

valid = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2024-03-05").cast("date"))
    .select(
        "*",
        F.lower("job_title").alias("job_title_lower"),
        F.initcap(F.trim("email")).alias("email_title"),
        F.coalesce("job_title", F.lit("NONE")).alias("resolved_job_title"),
    )
)

enriched = (
    valid
    .withColumn("prev_salary", F.lag("salary", 1, 0).over(w))
    .withColumn("prev_salary_2", F.lag("salary", 1).over(w2))
)

scored = (
    enriched
    .select(
        "*",
        F.sum("prev_salary_2").over(w_recent).alias("running_prev_salary_2"),
        F.avg("prev_salary").over(w4).alias("prev_salary_avg_2"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.sum("prev_salary_2").over(w6).alias("cumulative_prev_salary_2"),
    )
)

result = (
    scored
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        "job_title_lower",
        "email_title",
        F.col("min_employee_salary"),
    )
    .orderBy(F.col("level").desc_nulls_first())
)
