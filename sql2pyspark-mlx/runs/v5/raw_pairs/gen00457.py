from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "job_title").orderBy(F.col("hire_date").desc_nulls_first(), "id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.min("salary").alias("min_employee_salary"))

ranked = (
    df_employees
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "dept_id", "manager_id", "job_title", "hire_date", "is_remote")
)

result = (
    ranked
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select("id", "manager_id", "hire_date", "is_remote", F.col("min_employee_salary"))
    .orderBy(F.col("id").desc_nulls_first())
    .limit(5)
)
