from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("budget_rounded").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

valid = (
    df_departments
    .filter(F.col("location").isNotNull() & (~F.col("location").eqNullSafe(F.lit("New York"))))
    .select("*", F.round("budget", 1).alias("budget_rounded"))
)

with_windows = (
    valid
    .withColumn("running_budget", F.sum("budget").over(w))
    .withColumn("budget_rounded_percent_rank", F.percent_rank().over(w2))
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.budget",
        F.col("c.job_title").alias("employee_job_title"),
        F.col("c.salary"),
    )
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.salary") <= 85000), "left_anti")
)

result = (
    not_flagged
    .select("id", "location", "budget", "employee_job_title", "salary")
    .orderBy(F.col("employee_job_title").desc_nulls_last())
)
