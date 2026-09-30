from pyspark.sql import functions as F

valid = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-07-12"))) & F.col("level").isin(["L5", "L4", "L2"]))
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.when(F.col("salary") >= 123000, "High").when(F.col("salary") >= 86000, "Medium").otherwise("Low").alias("salary_level"),
        F.add_months("hire_date", 1).alias("hire_date_plus_months"),
        F.coalesce("level", F.lit("NONE")).alias("resolved_level"),
    )
)

with_dims = (
    valid
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id") == F.col("d.id"), "left")
    .filter(F.col("d.job_title").isin(["Intern", "Manager", "Director"]))
    .select(
        "src.id",
        "src.manager_id",
        "src.salary",
        "src.level",
        "src.resolved_level",
        F.col("d.email").alias("employee_email"),
        F.col("d.salary").alias("employee_salary"),
    )
)

with_dims_employees = (
    with_dims
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id") == F.col("d.id"), "inner")
    .select(
        "src.*",
        F.col("d.email"),
        F.col("d.salary").alias("employee_salary_2"),
        F.col("d.hire_date"),
    )
)

filtered = (
    with_dims_employees
    .filter((F.col("hire_date") > F.lit("2023-07-26").cast("date")) & (F.col("hire_date") < "2023-09-01"))
    .select(
        "id",
        "salary",
        "employee_email",
        "employee_salary",
        "email",
        F.lower("employee_email").alias("employee_email_lower"),
        F.coalesce("level", F.lit("N/A")).alias("level_clean"),
    )
)

base = (
    filtered
    .filter(F.col("salary") <= 93000)
    .withColumn("employee_salary_times_salary", (F.col("employee_salary") * F.col("salary")))
)

result = (
    base
    .select(
        "id",
        "salary",
        "employee_email",
        "email",
        "employee_email_lower",
        "level_clean",
        "employee_salary_times_salary",
    )
)
