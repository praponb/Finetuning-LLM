from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("location_dept_name", F.concat("location", F.lit(" - "), "dept_name"))
    .withColumn("dept_name_group", F.when(F.col("dept_name").isin(["Engineering", "Sales"]), "Core").otherwise("Other"))
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

base = (
    latest
    .filter(F.col("location") != "Berlin")
    .select(
        "id",
        "dept_name",
        "location_dept_name",
        "dept_name_group",
        F.floor("budget").alias("budget_whole"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.location_dept_name",
        "s.budget_whole",
        F.col("c.job_title").alias("employee_job_title"),
    )
)

result = (
    with_last
    .select("id", "location_dept_name")
    .orderBy(F.col("id").asc_nulls_first())
    .limit(20)
)
