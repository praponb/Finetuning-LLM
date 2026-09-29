from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("job_title", "manager_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent_2 = Window.partitionBy("job_title", "level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.max("salary").alias("max_employee_salary"))

scored = (
    df_employees
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.row_number().over(w_recent).alias("seq_no"),
        F.last("job_title", ignorenulls=True).over(w2).alias("last_known_job_title"),
        F.sum("salary").over(w_recent_2).alias("running_salary"),
    )
)

result = (
    scored
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "seq_no",
        "last_known_job_title",
        F.when(F.col("level").rlike("^(L3|L4)$"), "OTHER_ZONE").when(F.col("level").rlike("^(L1|L2)$"), "CORE").otherwise("EXTENDED").alias("level_zone"),
        F.col("max_employee_salary"),
    )
    .orderBy(F.col("level").asc_nulls_last())
)
