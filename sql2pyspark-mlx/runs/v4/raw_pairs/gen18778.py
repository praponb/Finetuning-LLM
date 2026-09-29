from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("dept_id", "level")
w3 = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy("dept_id", "manager_id")
w5 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.avg("salary").alias("avg_employee_salary"))

enriched = (
    df_employees
    .select(
        "*",
        F.ntile(5).over(w).alias("salary_bucket"),
        F.min("salary").over(w2).alias("dept_id_min_salary"),
        F.ntile(3).over(w3).alias("salary_bucket_2"),
        F.round((F.col("salary") / F.sum("salary").over(w4)), 4).alias("salary_share"),
        F.sum("salary").over(w5).alias("running_salary"),
    )
)

result = (
    enriched
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "dept_id_min_salary",
        F.col("avg_employee_salary"),
    )
    .orderBy(F.col("hire_date").desc())
)
