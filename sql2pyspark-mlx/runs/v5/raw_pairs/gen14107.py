from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("job_title", "manager_id").orderBy("hire_date", "id")
w_part = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w5 = Window.partitionBy("job_title").orderBy(F.col("hire_date").desc(), "id")

cleaned = (
    df_employees
    .filter((~F.col("job_title").eqNullSafe(F.lit("Engineer"))) & (F.col("salary") <= 96000))
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
        "level",
        F.substring("email", 1, 4).alias("email_prefix"),
    )
)

with_windows = (
    cleaned
    .withColumn("first_known_level", F.first("level", ignorenulls=True).over(w_grp))
    .withColumn("salary_avg_5_prior", F.avg("salary").over(w2))
    .withColumn("next_salary", F.lead("salary", 2).over(w3))
    .withColumn("salary_bucket", F.ntile(4).over(w_part))
)

base = (
    with_windows
    .filter(F.col("level").isNotNull() & (F.col("job_title").eqNullSafe(F.lit("Manager"))))
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "level",
        "email_prefix",
        "first_known_level",
        "salary_avg_5_prior",
        "next_salary",
        "salary_bucket",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.round("next_salary", 0).alias("next_salary_rounded"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .alias("s")
    .join(df_employees.alias("e"), F.col("s.manager_id") == F.col("e.id"), "inner")
    .groupBy(
        F.col("e.level").alias("employee_level"),
        F.col("e.job_title").alias("employee_job_title"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("s.salary_bucket")).alias("total_salary_bucket"),
    )
    .orderBy(F.col("employee_level").desc_nulls_last())
)
