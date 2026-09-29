from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.sum("salary").alias("sum_employee_salary"))

latest = (
    df_employees
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "hire_date",
        "level",
        F.col("sum_employee_salary"),
    )
    .orderBy(F.col("hire_date").desc_nulls_last(), F.col("level").desc())
)
