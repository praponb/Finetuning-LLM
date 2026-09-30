from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("salary", "id")
w_recent = Window.partitionBy("job_title", "manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("manager_id").orderBy("is_remote_flag", "id")
w4 = Window.orderBy(F.col("salary").desc(), "id")

valid = (
    df_employees
    .filter(F.col("hire_date").between("2023-10-01", "2024-12-01"))
    .select("*", F.upper("full_name").alias("full_name_upper"))
)

enriched = (
    valid
    .select(
        "*",
        F.ntile(4).over(w_running).alias("salary_bucket"),
        F.sum("salary").over(w_recent).alias("cumulative_salary"),
    )
)

base = (
    enriched
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-10-16"))) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        "full_name_upper",
        "salary_bucket",
        "cumulative_salary",
        F.when(F.col("job_title").isin(["Intern", "Analyst"]), "Tier 1").otherwise("Other").alias("job_title_group"),
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("level", F.lit("NONE")).alias("level_clean"),
    )
)

with_windows = (
    base
    .withColumn("next_salary_bucket", F.lead("salary_bucket", 1).over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)

remaining_keys = (
    with_windows
    .select("manager_id", "level")
    .subtract(
        df_employees
        .filter(F.col("dept_id").isin([5, 8, 16, 30]))
        .select("manager_id", "level")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.manager_id") == F.col("k.manager_id")) & (F.col("s.level") == F.col("k.level")), "inner")
    .select("s.*")
)

base_employees = (
    kept_rows
    .filter(F.col("job_title") == "Director")
    .select(
        "id",
        "email",
        "salary",
        "is_remote",
        "level",
        "full_name_upper",
        "cumulative_salary",
        "is_remote_flag",
        "level_clean",
        "next_salary_bucket",
        "event_seq",
        F.round((F.col("is_remote_flag") / F.col("salary_bucket")), 2).alias("is_remote_flag_per_salary_bucket"),
    )
)

result = (
    base_employees
    .select(
        "email",
        "full_name_upper",
        "is_remote_flag",
        "next_salary_bucket",
        "event_seq",
        "is_remote_flag_per_salary_bucket",
    )
    .orderBy(F.col("next_salary_bucket").desc_nulls_first())
)
