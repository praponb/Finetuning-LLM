from pyspark.sql import functions as F

enriched = (
    df_employees
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .filter(F.col("d.budget").between(2325000, 3035000))
    .select("a.*", F.col("d.budget"), F.col("d.dept_name"))
)

enriched_employees = (
    enriched
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.manager_id",
        "e.job_title",
        "e.hire_date",
        "e.is_remote",
        "e.level",
        "e.budget",
        F.col("d.hire_date").alias("employee_hire_date"),
        F.col("d.level").alias("employee_level"),
        F.col("d.salary").alias("employee_salary"),
    )
)

joined = (
    enriched_employees
    .alias("src")
    .join(df_employees.alias("b"), F.col("src.manager_id").eqNullSafe(F.col("b.id")), "left")
    .select("src.*", F.col("b.email"), F.col("b.level").alias("employee_level_2"))
)

enriched_employees_2 = (
    joined
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.manager_id",
        "a.job_title",
        "a.hire_date",
        "a.budget",
        "a.employee_hire_date",
        "a.employee_level",
        "a.employee_salary",
        "a.employee_level_2",
        F.col("d.level").alias("employee_level_3"),
        F.col("d.hire_date").alias("employee_hire_date_2"),
    )
)

result = (
    enriched_employees_2
    .groupBy("manager_id")
    .agg(
        F.percentile_approx("budget", [0.25, 0.5, 0.75]).alias("budget_quartiles"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("employee_level") == "L1").alias("l1_events"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("budget"), 2).alias("avg_budget"),
    )
    .filter(F.col("l1_events") < 1000)
    .orderBy(F.col("manager_id").desc())
)
