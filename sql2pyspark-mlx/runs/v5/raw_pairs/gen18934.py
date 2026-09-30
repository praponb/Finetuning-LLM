from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("job_title")
w3 = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level").orderBy("salary")
w5 = Window.partitionBy("level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.max("salary").alias("max_employee_salary"))

scored = (
    df_employees
    .withColumn("first_known_job_title", F.first("job_title", ignorenulls=True).over(w))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w2)), 4))
    .withColumn("last_known_level", F.last("level", ignorenulls=True).over(w3))
    .withColumn("salary_percent_rank", F.percent_rank().over(w4))
    .withColumn("first_known_job_title_2", F.first("job_title", ignorenulls=True).over(w5))
)

result = (
    scored
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "last_known_level",
        "salary_percent_rank",
        "first_known_job_title_2",
        F.col("max_employee_salary"),
    )
    .orderBy(F.col("salary_percent_rank").desc_nulls_last(), F.col("dept_id").desc())
)
