from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.avg("salary").alias("avg_employee_salary"))

ranked = (
    df_employees
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "row_num",
        F.col("avg_employee_salary"),
    )
)
