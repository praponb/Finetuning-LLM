from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")

cleaned = (
    df_employees
    .filter((F.col("salary") >= 106000) & (~F.col("job_title").eqNullSafe(F.lit("Designer"))))
    .select(
        "*",
        F.when(F.col("is_remote"), "Yes").otherwise("No").alias("is_remote_label"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("dept_id") == 27)
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.ntile(5).over(w2).alias("salary_bucket"),
    )
)

remaining_keys = (
    with_windows
    .select("id", "manager_id")
    .exceptAll(
        df_employees
        .filter(F.col("level") == "L3")
        .select("id", "manager_id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.manager_id") == F.col("k.manager_id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("src")
    .join(df_employees.alias("p"), F.col("src.manager_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.hire_date").alias("employee_hire_date"),
        F.col("p.level").alias("employee_level"),
        F.col("p.job_title").alias("employee_job_title"),
    )
)

result = (
    joined
    .select(
        "email",
        "dept_id",
        "manager_id",
        "is_remote",
        "level",
        "event_seq",
        F.when(F.col("level").rlike("^(L3|L2)$"), "GROUP_A").when(F.col("level").rlike("^(L1|L4)$"), "CORE").otherwise("PRIORITY").alias("level_zone"),
    )
)
