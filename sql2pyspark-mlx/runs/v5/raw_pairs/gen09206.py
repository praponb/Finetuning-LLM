from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "level").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.sum("salary").alias("sum_employee_salary"))

with_windows = (
    df_employees
    .filter(F.col("manager_id") == 39)
    .select(
        "*",
        F.ntile(10).over(w).alias("salary_bucket"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    with_windows
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "dept_id",
        "job_title",
        "salary",
        "level",
        "salary_bucket",
        "seq_no",
        F.col("sum_employee_salary"),
    )
    .orderBy(F.col("seq_no").desc_nulls_first())
)
