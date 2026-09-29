from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.orderBy("location_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("location").orderBy("location_length", "id")
w_grp = Window.partitionBy("dept_name").orderBy("location_length", "id")

cleaned = (
    df_departments
    .filter(F.col("dept_name").isin(["Engineering", "Sales"]))
    .select("*", F.length("location").alias("location_length"))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title").alias("employee_job_title"))
)

enriched = (
    with_latest
    .withColumn("first_known_dept_name", F.first("dept_name", ignorenulls=True).over(w2))
    .withColumn("next_budget", F.lead("budget", 2).over(w3))
    .withColumn("row_seq", F.row_number().over(w_grp))
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (~F.col("c.email").like("%@startup.io")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "location_length",
        "employee_job_title",
        "first_known_dept_name",
        "next_budget",
    )
    .orderBy(F.col("budget").desc())
)
