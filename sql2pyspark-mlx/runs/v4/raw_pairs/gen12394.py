from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "dept_id").orderBy("salary", "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.sum("salary").alias("sum_employee_salary"))

latest = (
    df_employees
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "email",
        "dept_id",
        "salary",
        "is_remote",
        "level",
        F.col("sum_employee_salary"),
    )
    .orderBy(F.col("dept_id").asc_nulls_last(), F.col("salary").desc())
)
