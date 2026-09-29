from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("employee_hire_date").desc_nulls_first(), "id")

with_dims = (
    df_employees
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id") == F.col("e.id"), "left")
    .select(
        "s.*",
        F.col("e.job_title").alias("employee_job_title"),
        F.col("e.email").alias("employee_email"),
    )
)

joined = (
    with_dims
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id") == F.col("d.id"), "inner")
    .select(
        "s.*",
        F.col("d.full_name").alias("employee_full_name"),
        F.col("d.hire_date").alias("employee_hire_date"),
    )
)

valid = (
    joined
    .filter((F.col("hire_date") > F.lit("2023-06-16").cast("date")) & F.col("employee_job_title").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        "employee_full_name",
        "employee_hire_date",
        F.concat_ws("/", "job_title", "full_name").alias("job_title_full_name_key"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

joined_employees = (
    deduped
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.budget"), F.col("d.location"))
)

result = (
    joined_employees
    .select(
        "id",
        "manager_id",
        "hire_date",
        "is_remote",
        "employee_full_name",
        "job_title_full_name_key",
        "rn",
        "budget",
        "location",
    )
)
