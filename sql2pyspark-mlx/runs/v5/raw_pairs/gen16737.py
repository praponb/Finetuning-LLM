from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc_nulls_first(), "id")
w_ordered = Window.partitionBy("location", "dept_name").orderBy("dedup_rank", "id")
w_running = Window.partitionBy("location", "dept_name").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("job_title").orderBy(F.col("salary").desc_nulls_last())
w6 = Window.partitionBy("job_title").orderBy(F.col("budget").desc_nulls_first())

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .withColumn("dept_name_upper", F.upper("dept_name"))
    .withColumn("location_filled", F.coalesce("location", F.lit("UNKNOWN")))
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    deduped
    .filter(F.col("location").isin(["Bangkok", "Singapore"]))
    .select(
        "id",
        "dept_name",
        "budget",
        "dept_name_upper",
        "location_filled",
        F.lag("budget").over(w_ordered).alias("prev_budget"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.sum("budget").over(w_running).alias("running_budget"),
    )
)

latest_match = (
    metrics
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.budget",
        "s.dept_name_upper",
        F.col("c.hire_date").alias("employee_hire_date"),
    )
)

kept = (
    latest_match
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")) & (F.col("c.hire_date") < F.col("s.employee_hire_date")), "left_semi")
)

latest_match_departments = (
    kept
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.budget",
        "s.dept_name_upper",
        "s.employee_hire_date",
        F.col("c.salary"),
        F.col("c.job_title"),
    )
)

scored = (
    latest_match_departments
    .select(
        "id",
        "budget",
        "dept_name_upper",
        "employee_hire_date",
        "salary",
        "job_title",
        F.dense_rank().over(w5).alias("salary_dense_rank"),
        F.rank().over(w6).alias("budget_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "budget",
        "dept_name_upper",
        "employee_hire_date",
        "salary",
        "job_title",
        "salary_dense_rank",
        F.when(F.col("job_title").rlike("^(Analyst|Director)$"), "CORE").when(F.col("job_title").rlike("^(Engineer|Manager)$"), "GROUP_B").otherwise("PRIORITY").alias("job_title_zone"),
    )
    .orderBy(F.col("job_title_zone").asc_nulls_last())
)
