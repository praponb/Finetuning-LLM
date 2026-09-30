from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc())
w2 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())
w3 = Window.partitionBy("dept_name", "dept_id").orderBy("hire_date", "id")
w4 = Window.partitionBy("dept_id").orderBy("salary", "id")
w_recent = Window.partitionBy("department_dept_name").orderBy(F.col("budget").desc_nulls_last())
w_running = Window.partitionBy("job_title").orderBy(F.col("salary_dense_rank").desc_nulls_last())
w_recent_2 = Window.partitionBy("department_dept_name")

cleaned = (
    df_employees
    .filter(F.col("level").isNotNull() & F.col("manager_id").isNull())
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
        F.length("full_name").alias("full_name_length"),
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
        F.coalesce("level", F.lit("OTHER")).alias("level_clean"),
    )
)

enriched = (
    cleaned
    .withColumn("salary_rank", F.rank().over(w))
    .withColumn("salary_dense_rank", F.dense_rank().over(w2))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_departments.alias("d"), F.col("a.dept_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.budget"), F.col("d.dept_name"))
)

with_dims_employees = (
    with_dims
    .alias("src")
    .join(df_departments.alias("p"), F.col("src.dept_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.dept_name").alias("department_dept_name"))
)

with_windows = (
    with_dims_employees
    .withColumn("prev_salary_dense_rank", F.lag("salary_dense_rank", 1, 0).over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
)

valid = (
    with_windows
    .filter(F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "full_name_length",
        "is_remote_label",
        "salary_rank",
        "salary_dense_rank",
        "budget",
        "department_dept_name",
        F.lower("department_dept_name").alias("department_dept_name_lower"),
    )
)

remaining_keys = (
    valid
    .select("dept_id", "id")
    .intersectAll(
        df_employees
        .filter(F.col("salary") > 73000)
        .select("dept_id", "id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "*",
        F.dense_rank().over(w_recent).alias("budget_dense_rank"),
        F.rank().over(w_running).alias("salary_dense_rank_rank"),
        F.avg("full_name_length").over(w_recent_2).alias("department_dept_name_avg_full_name_length"),
    )
)
