from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")
w2 = Window.partitionBy("manager_id")
w_part = Window.orderBy("hire_date", "id")
w_recent = Window.orderBy("salary", "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.sum("salary").alias("sum_employee_salary"))

with_windows = (
    df_employees
    .filter(F.col("level") == "L2")
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w)), 4))
    .withColumn("salary_share_2", F.round((F.col("salary") / F.sum("salary").over(w2)), 4))
    .withColumn("next_salary", F.lead("salary", 1).over(w_part))
    .withColumn("salary_bucket", F.ntile(4).over(w_recent))
)

result = (
    with_windows
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.col("sum_employee_salary"),
    )
    .orderBy(F.col("sum_employee_salary").desc())
)
