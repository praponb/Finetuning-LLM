from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("hire_date").desc_nulls_first())
w2 = Window.orderBy("hire_date", "id")
w3 = Window.partitionBy("job_title")
w_running = Window.partitionBy("employee_level").orderBy("employee_salary_share", "id").rowsBetween(-3, Window.currentRow)

enriched = (
    df_employees
    .alias("s")
    .join(df_employees.alias("d"), F.col("s.manager_id").eqNullSafe(F.col("d.id")), "left")
    .select(
        "s.id",
        "s.full_name",
        "s.manager_id",
        "s.job_title",
        "s.hire_date",
        "s.is_remote",
        "s.level",
        F.col("d.level").alias("employee_level"),
        F.col("d.email").alias("employee_email"),
        F.col("d.salary").alias("employee_salary"),
    )
)

deduped = (
    enriched
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    deduped
    .filter(F.lower("full_name").like("%any%"))
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "employee_level",
        "employee_email",
        "employee_salary",
        "latest_rank",
        F.row_number().over(w2).alias("row_seq"),
        F.count("employee_salary").over(w3).alias("job_title_count_employee_salary"),
        F.round((F.col("employee_salary") / F.sum("employee_salary").over(w3)), 4).alias("employee_salary_share"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "full_name",
        "manager_id",
        "job_title",
        "employee_level",
        "employee_salary_share",
        F.min("employee_salary_share").over(w_running).alias("last4_min_employee_salary_share"),
        F.count("employee_salary_share").over(w_running).alias("last4_count_employee_salary_share"),
        F.max("employee_salary_share").over(w_running).alias("last4_max_employee_salary_share"),
    )
)

joined = (
    scored
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.manager_id",
        "a.job_title",
        "a.employee_level",
        "a.employee_salary_share",
        F.col("p.hire_date"),
    )
)

result = (
    joined
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "left")
    .groupBy(
        F.col("p.level").alias("level"),
        F.col("e.employee_level").alias("employee_level"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("e.employee_salary_share")), 2).alias("total_employee_salary_share"),
    )
)
