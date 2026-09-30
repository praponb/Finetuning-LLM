from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("employee_job_title").orderBy("budget", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("employee_job_title").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_name").orderBy(F.col("budget").asc_nulls_first())
w_grp = Window.orderBy("budget", "id")

filtered = (
    df_departments
    .filter(F.col("budget") > 1409000)
    .withColumn("dept_name_title", F.initcap(F.trim("dept_name")))
    .withColumn("location_clean", F.coalesce("location", F.lit("UNKNOWN")))
)

filtered_departments = (
    filtered
    .filter(F.col("location") != "Chiang Mai")
    .select(
        "id",
        "dept_name",
        "budget",
        "dept_name_title",
        F.when(F.col("location").isin(["New York", "Tokyo"]), "Group A").otherwise("Other").alias("location_group"),
        F.coalesce("dept_name", F.lit("NONE")).alias("resolved_dept_name"),
    )
)

with_latest = (
    filtered_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.job_title").alias("employee_job_title"))
)

with_windows = (
    with_latest
    .withColumn("budget_avg_2", F.avg("budget").over(w2))
    .withColumn("cumulative_budget", F.sum("budget").over(w3))
)

with_windows_departments = (
    with_windows
    .withColumn("budget_rank", F.rank().over(w4))
    .withColumn("prev_cumulative_budget", F.lag("cumulative_budget", 1).over(w_grp))
)

result = (
    with_windows_departments
    .select(
        "id",
        "budget",
        "dept_name_title",
        "location_group",
        "employee_job_title",
        "budget_avg_2",
        "cumulative_budget",
        F.when(F.col("employee_job_title").rlike("^(Designer|Manager)$"), "GROUP_A").when(F.col("employee_job_title").rlike("^(Engineer|Analyst)$"), "EXTENDED").otherwise("PRIORITY").alias("employee_job_title_zone"),
    )
    .orderBy(F.col("id").desc())
)
