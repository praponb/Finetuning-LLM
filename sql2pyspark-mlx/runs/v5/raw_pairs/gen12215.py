from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("budget").asc_nulls_last())
w_ordered = Window.partitionBy("location", "dept_name").orderBy("budget", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("dept_name").orderBy(F.col("budget").desc_nulls_last())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

with_windows = (
    df_departments
    .filter(F.col("budget") < 1192000)
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.rank().over(w).alias("budget_rank"),
        F.avg("budget").over(w_ordered).alias("budget_avg_3"),
        F.percent_rank().over(w3).alias("budget_percent_rank"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.budget_rank",
        "s.budget_avg_3",
        "s.budget_percent_rank",
        F.col("c.job_title"),
        F.col("c.hire_date"),
    )
)

kept = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") <= F.col("s.hire_date")), "left_semi")
)

latest_match_departments = (
    kept
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.budget_rank",
        "s.budget_avg_3",
        "s.budget_percent_rank",
        "s.job_title",
        "s.hire_date",
        F.col("c.level").alias("employee_level"),
        F.col("c.job_title").alias("employee_job_title"),
    )
)

result = (
    latest_match_departments
    .groupBy("employee_job_title")
    .agg(
        F.count(F.when(F.col("job_title") == "Director", 1)).alias("director_rows"),
        F.sort_array(F.collect_set("employee_level")).alias("employee_level_list"),
    )
)
