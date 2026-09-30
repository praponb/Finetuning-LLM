from pyspark.sql import functions as F

base = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-04-15"))) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.add_months("hire_date", 6).alias("hire_date_plus_months"),
        F.floor("salary").alias("salary_whole"),
    )
)

result = (
    base
    .select("level", "manager_id")
    .exceptAll(
        df_employees
        .filter(F.col("salary") >= 72000)
        .select("level", "manager_id")
    )
)
