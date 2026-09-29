from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("location").orderBy(F.col("budget").asc_nulls_last(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("dept_name_prefix", F.substring("dept_name", 1, 2))
    .withColumn("budget_int", F.col("budget").cast("int"))
    .withColumn("location_filled", F.coalesce("location", F.lit("OTHER")))
)

cleaned = (
    filtered
    .filter(F.col("dept_name") != "Finance")
    .withColumn("budget_times_budget_int", (F.col("budget") * F.col("budget_int")))
    .withColumn("budget_int_rounded", F.round("budget_int", 2))
    .withColumn("resolved_location", F.coalesce("location", F.lit("OTHER")))
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.location",
        "s.budget",
        "s.budget_int",
        "s.location_filled",
        "s.budget_times_budget_int",
        "s.budget_int_rounded",
        "s.resolved_location",
        F.col("c.hire_date"),
    )
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.salary").alias("employee_salary"))
)

latest = (
    latest_match
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

with_last = (
    latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.budget",
        "s.budget_int",
        "s.resolved_location",
        "s.employee_salary",
        "s.latest_rank",
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

latest_match_departments = (
    with_last
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.hire_date").alias("employee_hire_date_2"))
)

result = (
    latest_match_departments
    .groupBy(F.date_trunc("MONTH", "employee_hire_date").alias("activity_month"))
    .agg(
        F.percentile_approx("employee_salary", 0.95).alias("employee_salary_p95"),
        F.round(F.avg("budget_int"), 2).alias("avg_budget_int"),
        F.sum("employee_salary").alias("total_employee_salary"),
    )
    .orderBy(F.col("avg_budget_int").desc())
)
