from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("dept_name").orderBy("employee_hire_date", "id")
w3 = Window.partitionBy("dept_name").orderBy("employee_hire_date_dayofweek", "id")

latest_match = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title"), F.col("c.salary"))
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.dept_name", F.col("c.hire_date").alias("employee_hire_date"))
)

deduped = (
    with_latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

base = (
    deduped
    .filter(F.col("dept_name").isNotNull() & F.col("employee_hire_date").between("2024-07-27", "2024-10-01"))
    .withColumn("employee_hire_date_dayofweek", F.dayofweek("employee_hire_date"))
)

deduped_departments = (
    base
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "employee_hire_date", "rn")
)

result = deduped_departments.select("id", "employee_hire_date")
