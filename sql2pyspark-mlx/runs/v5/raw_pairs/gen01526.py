from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

with_latest = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.budget",
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

with_last = (
    with_latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.dept_name", "s.employee_hire_date", F.col("c.level"))
)

with_latest_departments = (
    with_last
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.employee_hire_date",
        "s.level",
        F.col("c.job_title").alias("employee_job_title"),
    )
)

latest_match = (
    with_latest_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.employee_hire_date",
        "s.level",
        "s.employee_job_title",
        F.col("c.job_title").alias("employee_job_title_2"),
    )
)

result = (
    latest_match
    .rollup(
        F.date_trunc("MONTH", "employee_hire_date").alias("activity_month"),
        F.date_trunc("QUARTER", "employee_hire_date").alias("quarter_start"),
        "employee_job_title",
    )
    .agg(
        F.count(F.when(F.col("employee_job_title") == "Director", 1)).alias("director_count"),
        F.countDistinct("employee_job_title").alias("distinct_employee_job_titles"),
        F.sort_array(F.collect_set("employee_job_title")).alias("employee_job_title_list"),
        F.count("*").alias("cnt"),
        F.countDistinct("employee_job_title").alias("distinct_employee_job_titles_2"),
        F.count("*").alias("event_count"),
    )
    .orderBy("employee_job_title")
)
