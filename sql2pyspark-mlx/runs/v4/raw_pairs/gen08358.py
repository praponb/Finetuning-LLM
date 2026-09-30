from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("hire_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("location", "dept_id").orderBy("employee_salary", "id")
w4 = Window.partitionBy("level").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("hire_date", "id")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.max("salary").alias("max_employee_salary"))

with_dims = (
    df_employees
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id") == F.col("d.id"), "left")
    .select(
        "src.*",
        F.col("d.salary").alias("employee_salary"),
        F.col("d.hire_date").alias("employee_hire_date"),
        F.col("d.level").alias("employee_level"),
    )
)

with_dims_employees = (
    with_dims
    .alias("e")
    .join(df_employees.alias("b"), F.col("e.manager_id") == F.col("b.id"), "inner")
    .select(
        "e.*",
        F.col("b.email").alias("employee_email"),
        F.col("b.salary").alias("employee_salary_2"),
    )
)

with_dims_employees_2 = (
    with_dims_employees
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.location"), F.col("d.dept_name"))
)

with_dims_employees_3 = (
    with_dims_employees_2
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.budget"))
)

with_windows = (
    with_dims_employees_3
    .withColumn("running_salary", F.sum("salary").over(w))
    .withColumn("employee_salary_2_avg_6", F.avg("employee_salary_2").over(w2))
)

metrics = (
    with_windows
    .withColumn("employee_salary_bucket", F.ntile(3).over(w3))
    .withColumn("cumulative_budget", F.sum("budget").over(w4))
    .withColumn("event_seq", F.row_number().over(w5))
)

result = (
    metrics
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
        "is_remote",
        F.when(F.col("full_name").rlike("^(Jack Taylor|Alice Wong)$"), "OTHER_ZONE").when(F.col("full_name").rlike("^(bob smith|  Carol Diaz)$"), "GROUP_B").otherwise("PRIORITY").alias("full_name_zone"),
        F.col("max_employee_salary"),
    )
)
