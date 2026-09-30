from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())
w4 = Window.partitionBy("manager_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("level").orderBy(F.col("salary_rounded").desc())
w_running = Window.partitionBy("dept_id").orderBy(F.col("salary_rounded").desc(), "id")

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-03-16").cast("date")) & F.col("email").isNotNull())
    .withColumn("salary_rounded", F.round("salary", 2))
    .withColumn("resolved_job_title", F.coalesce("job_title", F.lit("N/A")))
)

ranked = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_windows = (
    ranked
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w2)), 4))
    .withColumn("salary_dense_rank", F.dense_rank().over(w3))
    .withColumn("first_known_job_title", F.first("job_title", ignorenulls=True).over(w4))
    .withColumn("salary_rounded_percent_rank", F.percent_rank().over(w5))
    .withColumn("salary_rounded_bucket", F.ntile(4).over(w_running))
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.dept_id",
        "s.job_title",
        "s.is_remote",
        "s.level",
        "s.salary_rounded",
        "s.resolved_job_title",
        "s.salary_dense_rank",
        "s.salary_rounded_percent_rank",
        "s.salary_rounded_bucket",
        F.col("d.budget"),
        F.col("d.dept_name"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "dept_id",
        F.col("salary_rounded").alias("SALARY_ROUNDED_VAL"),
        F.col("budget").alias("BUDGET"),
        F.col("salary_rounded_percent_rank").alias("SALARY_ROUNDED_PERCENT_RANK"),
    )
    .unpivot(
        ids=["id", "dept_id"],
        values=["SALARY_ROUNDED_VAL", "BUDGET", "SALARY_ROUNDED_PERCENT_RANK"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
