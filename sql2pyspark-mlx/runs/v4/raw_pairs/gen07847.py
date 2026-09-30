from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "job_title").orderBy("is_remote_flag", "id")
w2 = Window.partitionBy("level").orderBy("is_remote_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("dept_id").orderBy(F.col("is_remote_flag").asc_nulls_first())

valid = (
    df_employees
    .filter(F.col("level").isNotNull() & F.col("manager_id").isNotNull())
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "level",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.upper("level").alias("level_upper"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_clean"),
    )
)

with_windows = (
    valid
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("last_known_is_remote_flag", F.last("is_remote_flag", ignorenulls=True).over(w2))
    .withColumn("is_remote_flag_cume_dist", F.cume_dist().over(w_recent))
)

remaining_keys = (
    with_windows
    .select("dept_id", "job_title")
    .intersect(
        df_employees
        .filter(F.col("dept_id") == 24)
        .select("dept_id", "job_title")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("s")
    .join(df_departments.alias("p"), F.col("s.dept_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.dept_name"))
)

kept_keys = (
    with_dims
    .select("id")
    .intersect(
        df_employees
        .filter(F.col("dept_id") == 12)
        .select("id")
    )
)

verified_employees = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified_employees
    .filter(F.col("dept_id") == 16)
    .select(
        "*",
        F.concat_ws("-", "dept_name", "level").alias("dept_name_level_key"),
        F.upper("dept_name").alias("dept_name_upper"),
    )
)

with_dims_employees = (
    base
    .alias("s")
    .join(df_employees.alias("b"), F.col("s.manager_id") == F.col("b.id"), "inner")
    .select(
        "s.*",
        F.col("b.hire_date"),
        F.col("b.full_name").alias("employee_full_name"),
        F.col("b.email"),
    )
)

result = (
    with_dims_employees
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "job_title",
        "level",
        "is_remote_flag",
        "level_upper",
        "seq_no",
        F.when(F.col("job_title").rlike("^(Manager|Intern)$"), "GROUP_B").otherwise("EXTENDED").alias("job_title_zone"),
    )
    .orderBy(F.col("job_title_zone").desc())
)
