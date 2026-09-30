from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("employee_job_title").orderBy(F.col("job_title_length").desc_nulls_last(), "id")

base = (
    df_employees
    .filter(F.col("hire_date") >= F.lit("2023-03-28").cast("date"))
    .select(
        "*",
        F.length("job_title").alias("job_title_length"),
        F.coalesce("job_title", F.lit("NONE")).alias("resolved_job_title"),
    )
)

enriched = (
    base
    .alias("s")
    .join(df_employees.alias("b"), F.col("s.manager_id") == F.col("b.id"), "left")
    .filter(F.col("b.is_remote"))
    .select(
        "s.*",
        F.col("b.hire_date").alias("employee_hire_date"),
        F.col("b.job_title").alias("employee_job_title"),
        F.col("b.level").alias("employee_level"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

joined = (
    ranked
    .alias("src")
    .join(df_employees.alias("d"), F.col("src.manager_id") == F.col("d.id"), "left")
    .select(
        "src.*",
        F.col("d.full_name").alias("employee_full_name"),
        F.col("d.email").alias("employee_email"),
    )
)

with_dims = (
    joined
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.dept_name"), F.col("d.budget"), F.col("d.location"))
)

result = (
    with_dims
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["employee_job_title", "employee_employee_job_title"], ["employee_job_title"]],
        F.col("p.job_title").alias("employee_job_title"),
        F.col("e.employee_job_title").alias("employee_employee_job_title"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("e.salary")).alias("total_salary"),
        F.sum(F.col("e.budget")).alias("total_budget"),
        F.grouping_id().alias("gid"),
    )
)
