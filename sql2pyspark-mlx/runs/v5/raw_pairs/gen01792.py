from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.sum("salary").alias("sum_employee_salary"))

ranked = (
    df_employees
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "dept_id", "manager_id", "job_title", "hire_date", "is_remote")
)

result = (
    ranked
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        F.when(F.col("job_title").rlike("^(Manager|Director)$"), "EXTENDED").when(F.col("job_title").rlike("^(Engineer|Analyst)$"), "GROUP_A").otherwise("GROUP_B").alias("job_title_zone"),
        F.col("sum_employee_salary"),
    )
    .orderBy("manager_id", F.col("sum_employee_salary").desc())
)
