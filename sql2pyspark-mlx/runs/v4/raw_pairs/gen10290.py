from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("level")
w2 = Window.partitionBy("manager_id").orderBy("salary", "id")

with_dims = (
    df_employees
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id") == F.col("d.id"), "left")
    .select(
        "src.*",
        F.col("d.email").alias("employee_email"),
        F.col("d.full_name").alias("employee_full_name"),
    )
)

with_windows = (
    with_dims
    .withColumn("level_max_salary", F.max("salary").over(w_hist))
    .withColumn("next_salary", F.lead("salary", 1).over(w2))
)

joined = (
    with_windows
    .alias("src")
    .join(df_departments.alias("b"), F.col("src.dept_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.dept_name"), F.col("b.location"), F.col("b.budget"))
)

with_dims_employees = (
    joined
    .alias("e")
    .join(df_employees.alias("b"), F.col("e.manager_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.job_title").alias("employee_job_title"))
)

result = (
    with_dims_employees
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
    )
)
