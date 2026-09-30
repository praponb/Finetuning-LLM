from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.max("salary").alias("max_employee_salary"))

enriched = (
    df_employees
    .select(
        "*",
        F.count("salary").over(w).alias("running_count_salary"),
        F.min("salary").over(w).alias("running_min_salary"),
        F.avg("salary").over(w).alias("running_avg_salary"),
    )
)

result = (
    enriched
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "level",
        "running_count_salary",
        "running_min_salary",
        "running_avg_salary",
        F.when(F.col("job_title").rlike("^(Analyst|Designer)$"), "PRIORITY").otherwise("GROUP_B").alias("job_title_zone"),
        F.col("max_employee_salary"),
    )
)
