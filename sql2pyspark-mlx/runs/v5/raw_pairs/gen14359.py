from pyspark.sql import functions as F

base = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-06-07").cast("date")) & (F.col("salary") >= 139000))
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "hire_date",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)

result = (
    base
    .alias("a")
    .join(df_employees.alias("d"), F.col("a.manager_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.manager_id",
        "a.is_remote_label",
        F.col("d.job_title"),
        F.col("d.email"),
        F.col("d.salary"),
    )
)
