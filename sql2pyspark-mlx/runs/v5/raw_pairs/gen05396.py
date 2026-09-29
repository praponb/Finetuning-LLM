from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-07-03").cast("date")) & (~F.col("job_title").eqNullSafe(F.lit("Designer"))))
    .select(
        "*",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.col("salary").cast("int").alias("salary_int"),
    )
)

joined = (
    cleaned
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "left")
    .filter(F.col("p.hire_date") <= "2024-09-01")
    .select(
        "a.*",
        F.col("p.hire_date").alias("employee_hire_date"),
        F.col("p.salary").alias("employee_salary"),
        F.col("p.email").alias("employee_email"),
    )
)

filtered = (
    joined
    .filter((F.col("hire_date") > F.lit("2023-07-15").cast("date")) & F.col("job_title").isNotNull())
    .withColumn("is_l1", F.when(F.col("level") == "L1", 1).otherwise(0))
    .withColumn("level_clean", F.coalesce("level", F.lit("UNKNOWN")))
)

with_dims = (
    filtered
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id").eqNullSafe(F.col("d.id")), "inner")
    .select(
        "e.id",
        "e.email",
        "e.manager_id",
        "e.job_title",
        "e.employee_hire_date",
        "e.employee_salary",
        F.col("d.level").alias("employee_level"),
        F.col("d.hire_date").alias("employee_hire_date_2"),
    )
)

totals = (
    with_dims
    .groupBy(
        "employee_level",
        F.when(F.col("employee_salary") >= 122000, "LARGE").otherwise("SMALL").alias("employee_salary_tier"),
    )
    .agg(
        F.sum(F.when(F.col("employee_level") == "L2", F.col("employee_salary")).otherwise(0)).alias("l2_employee_salary"),
        F.sum(F.when(F.col("job_title") == "Director", F.col("employee_salary")).otherwise(0)).alias("director_employee_salary"),
    )
)

result = (
    totals
    .select(
        "employee_level",
        "employee_salary_tier",
        "l2_employee_salary",
        "director_employee_salary",
    )
)
