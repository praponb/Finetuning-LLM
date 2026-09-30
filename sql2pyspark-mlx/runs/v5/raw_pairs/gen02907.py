from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id")
w2 = Window.partitionBy("manager_id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.min("salary").alias("min_employee_salary"))

enriched = (
    df_employees
    .select(
        "*",
        F.lag("salary", 1).over(w).alias("prev_salary"),
        F.avg("salary").over(w2).alias("manager_id_avg_salary"),
    )
)

result = (
    enriched
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.when(F.col("job_title").rlike("^(Analyst|Intern)$"), "OTHER_ZONE").when(F.col("job_title").rlike("^(Engineer|Manager)$"), "GROUP_A").otherwise("GROUP_B").alias("job_title_zone"),
        F.col("min_employee_salary"),
    )
)
