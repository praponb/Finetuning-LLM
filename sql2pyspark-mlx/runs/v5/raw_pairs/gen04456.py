from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id")

filtered = (
    df_employees
    .filter(F.col("salary") < 139000)
    .withColumn("job_title_title", F.initcap(F.trim("job_title")))
    .withColumn("salary_whole", F.ceil("salary"))
)

with_dims = (
    filtered
    .alias("e")
    .join(df_employees.alias("p"), F.col("e.manager_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.full_name",
        "e.dept_id",
        "e.manager_id",
        "e.job_title",
        "e.salary",
        "e.level",
        "e.salary_whole",
        F.col("p.email").alias("employee_email"),
        F.col("p.hire_date").alias("employee_hire_date"),
    )
)

deduped = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "manager_id",
        "job_title",
        "salary",
        "salary_whole",
        "employee_hire_date",
    )
)

matched_keys = (
    deduped
    .select("manager_id")
    .intersectAll(
        df_employees
        .filter(F.lower(F.trim("email")) == "carol@company.co")
        .select("manager_id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

remaining_keys = (
    kept_rows
    .select("id", "job_title")
    .subtract(
        df_employees
        .filter(~F.col("is_remote"))
        .select("id", "job_title")
    )
)

verified = (
    kept_rows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

result = (
    verified
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["employee_job_title", "level"], ["employee_job_title"], []],
        F.col("p.job_title").alias("employee_job_title"),
        F.col("p.level").alias("level"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.salary")).alias("max_salary"),
        F.max(F.col("src.salary_whole")).alias("max_salary_whole"),
    )
)
