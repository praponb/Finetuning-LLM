from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("budget_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")
w3 = Window.orderBy(F.col("budget").asc_nulls_first())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.salary").desc(), F.col("c.id").desc())

base = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("location") == "Bangkok"))
    .withColumn("budget_int", F.col("budget").cast("int"))
    .withColumn("dept_name_location", F.concat("dept_name", F.lit(" - "), "location"))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("UNKNOWN")))
)

with_windows = (
    base
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        "budget_int",
        F.last("dept_name", ignorenulls=True).over(w).alias("last_known_dept_name"),
        F.row_number().over(w2).alias("row_seq"),
        F.dense_rank().over(w3).alias("budget_dense_rank"),
    )
)

with_latest = (
    with_windows
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        "s.row_seq",
        "s.budget_dense_rank",
        F.col("c.level").alias("employee_level"),
        F.col("c.salary"),
    )
)

with_last = (
    with_latest
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.row_seq",
        "s.employee_level",
        F.col("c.salary").alias("employee_salary"),
    )
)

cleaned = (
    with_last
    .filter(F.col("employee_level").isNotNull())
    .withColumn("row_seq_scaled", F.round((F.col("row_seq") * 1.07), 2))
    .withColumn("row_seq_minus_employee_salary", (F.col("row_seq") - F.col("employee_salary")))
    .withColumn("employee_level_clean", F.coalesce("employee_level", F.lit("OTHER")))
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.level").alias("employee_level_2"),
        F.col("c.job_title").alias("employee_job_title"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "row_seq",
        "employee_salary",
        "row_seq_minus_employee_salary",
        "employee_level_clean",
        "employee_job_title",
    )
)
