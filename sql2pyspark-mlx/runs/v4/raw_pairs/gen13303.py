from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id")
w2 = Window.partitionBy("manager_id", "level").orderBy("salary", "id")
w3 = Window.partitionBy("level", "manager_id")
w4 = Window.orderBy("hire_date", "id")
w5 = Window.partitionBy("employee_level", "manager_id").orderBy("prev_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("prev_salary").desc(), "id")

enriched = (
    df_employees
    .withColumn("prev_salary", F.lag("salary", 2).over(w))
    .withColumn("prev_salary_2", F.lag("salary", 2).over(w2))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w3)), 4))
    .withColumn("next_salary", F.lead("salary", 1).over(w4))
)

joined = (
    enriched
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.dept_name"))
)

enriched_employees = (
    joined
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.email").alias("employee_email"),
        F.col("d.salary").alias("employee_salary"),
        F.col("d.level").alias("employee_level"),
    )
)

with_windows = (
    enriched_employees
    .withColumn("running_employee_salary", F.sum("employee_salary").over(w5))
    .withColumn("prev_salary_bucket", F.ntile(5).over(w6))
)

result = (
    with_windows
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "prev_salary_2",
    )
    .orderBy(F.col("dept_id").asc_nulls_first())
)
