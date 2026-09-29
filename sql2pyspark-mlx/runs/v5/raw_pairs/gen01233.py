from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary").desc(), "id")
w2 = Window.partitionBy("level").orderBy(F.col("hire_date").desc_nulls_last(), "id")
w3 = Window.partitionBy("job_title", "manager_id").orderBy(F.col("employee_salary").desc(), "id")
w4 = Window.partitionBy("manager_id").orderBy("employee_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("manager_id", "level").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

latest = (
    df_employees
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest_employees = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "dept_id", "manager_id", "job_title", "salary", "hire_date")
)

with_dims = (
    latest_employees
    .alias("src")
    .join(df_employees.alias("b"), F.col("src.manager_id") == F.col("b.id"), "left")
    .filter(F.month(F.col("b.hire_date")).isin([7, 8, 11]))
    .select(
        "src.*",
        F.col("b.salary").alias("employee_salary"),
        F.col("b.job_title").alias("employee_job_title"),
        F.col("b.email"),
    )
)

filtered = (
    with_dims
    .filter(F.col("email").like("%@startup.io"))
    .select(
        "*",
        F.concat_ws("/", "email", "job_title").alias("email_job_title_key"),
        F.coalesce("employee_job_title", F.lit("N/A")).alias("employee_job_title_filled"),
    )
)

enriched = (
    filtered
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.manager_id",
        "src.job_title",
        "src.salary",
        "src.hire_date",
        "src.employee_salary",
        "src.email",
        "src.employee_job_title_filled",
        F.col("p.full_name"),
        F.col("p.level"),
        F.col("p.hire_date").alias("employee_hire_date"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "manager_id",
        "employee_salary",
        "employee_job_title_filled",
        "full_name",
        "level",
        F.row_number().over(w3).alias("row_seq"),
        F.last("employee_salary", ignorenulls=True).over(w4).alias("last_known_employee_salary"),
        F.sum("salary").over(w5).alias("cumulative_salary"),
    )
)

totals = (
    with_windows
    .groupBy(
        "manager_id",
        F.when(F.col("last_known_employee_salary") >= 125000, "HIGH").otherwise("LOW").alias("last_known_employee_salary_tier"),
        "level",
    )
    .agg(
        F.avg("employee_salary").alias("avg_employee_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.round(F.sum("row_seq") / F.nullif(F.count(F.when(F.col("level") == "L5", 1)), F.lit(0)), 2).alias("row_seq_per_l5"),
    )
)

result = (
    totals
    .select(
        "manager_id",
        "last_known_employee_salary_tier",
        "level",
        "avg_employee_salary",
    )
)
