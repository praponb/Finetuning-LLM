from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "employee_job_title").orderBy(F.col("hire_date").desc())
w2 = Window.partitionBy("employee_job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_id").orderBy("rn", "id")
w5 = Window.partitionBy("dept_id", "manager_id").orderBy("seq_no")
w6 = Window.partitionBy("dept_id").orderBy(F.col("seq_no").desc_nulls_first())

joined = (
    df_employees
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "left")
    .select(
        "a.id",
        "a.email",
        "a.dept_id",
        "a.manager_id",
        "a.hire_date",
        "a.is_remote",
        F.col("e.email").alias("employee_email"),
        F.col("e.full_name").alias("employee_full_name"),
        F.col("e.job_title").alias("employee_job_title"),
    )
)

deduped = (
    joined
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    deduped
    .filter(F.col("employee_full_name").like("C%"))
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "hire_date",
        "is_remote",
        "employee_full_name",
        "rn",
        F.last("employee_job_title", ignorenulls=True).over(w2).alias("last_known_employee_job_title"),
        F.first("employee_job_title", ignorenulls=True).over(w3).alias("first_known_employee_job_title"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

scored = (
    metrics
    .select(
        "*",
        F.percent_rank().over(w5).alias("seq_no_percent_rank"),
        F.percent_rank().over(w6).alias("seq_no_percent_rank_2"),
    )
)

result = (
    scored
    .select(
        "id",
        "email",
        "hire_date",
        "is_remote",
        "employee_full_name",
        "rn",
        "first_known_employee_job_title",
        "seq_no",
        "seq_no_percent_rank",
    )
    .orderBy("seq_no_percent_rank", "hire_date")
)
