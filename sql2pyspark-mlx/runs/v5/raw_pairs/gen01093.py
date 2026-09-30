from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location", "dept_name").orderBy("running_max_budget", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_name", "location").orderBy(F.col("running_min_budget").desc(), "id")
w4 = Window.orderBy("running_min_budget", "id")
w5 = Window.orderBy("running_max_budget", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

metrics = (
    df_departments
    .withColumn("running_sum_budget", F.sum("budget").over(w))
    .withColumn("running_max_budget", F.max("budget").over(w))
    .withColumn("running_min_budget", F.min("budget").over(w))
)

kept = (
    metrics
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.email").isNotNull()), "left_anti")
)

with_windows = (
    kept
    .select(
        "*",
        F.first("running_max_budget", ignorenulls=True).over(w2).alias("first_known_running_max_budget"),
        F.ntile(4).over(w3).alias("running_min_budget_bucket"),
        F.lead("running_sum_budget", 1).over(w4).alias("next_running_sum_budget"),
        F.avg("running_sum_budget").over(w5).alias("running_sum_budget_avg_2"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.running_sum_budget",
        "s.running_min_budget",
        F.col("c.hire_date"),
    )
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.running_sum_budget",
        "s.running_min_budget",
        "s.hire_date",
        F.col("c.job_title").alias("employee_job_title"),
    )
)

with_last = (
    with_latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary"))
)

result = (
    with_last
    .groupBy(
        F.when(F.col("running_sum_budget") >= 18234000, "PREMIUM").otherwise("STANDARD").alias("running_sum_budget_tier"),
        F.date_trunc("MONTH", "hire_date").alias("activity_month"),
        F.when(F.col("salary") >= 129000, "LARGE").otherwise("SMALL").alias("salary_tier"),
    )
    .agg(
        F.sum(F.when(F.col("employee_job_title") == "Manager", F.col("running_sum_budget")).otherwise(0)).alias("manager_running_sum_budget"),
        F.count(F.when(F.col("employee_job_title") == "Designer", 1)).alias("designer_count"),
        F.percentile_approx("running_sum_budget", 0.5).alias("running_sum_budget_p50"),
    )
    .filter(F.col("designer_count") >= 1)
)
