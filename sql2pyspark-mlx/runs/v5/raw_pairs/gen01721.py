from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

eligible = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.salary").between(72000, 91000)), "left_semi")
)

with_last = (
    eligible
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary").alias("employee_salary"))
)

latest_match = (
    with_last
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.level").alias("employee_level"),
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

kept = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.length(F.col("c.email")) > 13), "left_semi")
)

latest_match_departments = (
    kept
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.location", "s.employee_salary", F.col("c.salary"))
)

result = (
    latest_match_departments
    .select("id", "employee_salary", "salary")
    .orderBy(F.col("employee_salary").desc())
)
