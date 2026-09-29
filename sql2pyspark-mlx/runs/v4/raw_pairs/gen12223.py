from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("location_upper", F.upper("location"))
)

with_last = (
    valid
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary").alias("employee_salary"))
)

with_last_departments = (
    with_last
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary").alias("employee_salary_2"))
)

cleaned = (
    with_last_departments
    .filter(F.col("employee_salary").between(57000, 131000))
    .select(
        "id",
        "dept_name",
        "employee_salary",
        "employee_salary_2",
        F.col("budget").cast("int").alias("budget_int"),
        F.coalesce("dept_name", F.lit("UNKNOWN")).alias("dept_name_filled"),
    )
)

result = (
    cleaned
    .select("id", "dept_name", "employee_salary", "employee_salary_2")
    .unpivot(
        ["id", "dept_name"],
        ["employee_salary", "employee_salary_2"],
        "metric_name",
        "val",
    )
    .filter(F.col("val").isNotNull())
)
