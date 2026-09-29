from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy(F.col("salary").desc(), "id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.min("salary").alias("min_employee_salary"))

deduped = (
    df_employees
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    deduped
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        F.col("min_employee_salary"),
    )
)
