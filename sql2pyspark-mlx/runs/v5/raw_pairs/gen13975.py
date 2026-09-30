from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.hire_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("job_title", "dept_name").orderBy(F.col("employee_hire_date").desc_nulls_first(), "id")
w3 = Window.partitionBy("job_title").orderBy(F.col("rank_in_group").desc(), "id")

with_last = (
    df_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.dept_name",
        F.col("c.hire_date").alias("employee_hire_date"),
        F.col("c.job_title"),
    )
)

ranked = (
    with_last
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

ranked_departments = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "employee_hire_date", "job_title", "rank_in_group")
)

latest_match = (
    ranked_departments
    .alias("s")
    .join(df_employees.alias("c"), (F.col("c.dept_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.level").alias("employee_level"),
        F.col("c.job_title").alias("employee_job_title"),
    )
)

result = (
    latest_match
    .select("id", "job_title", "employee_level", "employee_job_title")
    .orderBy(F.col("employee_level").desc_nulls_last())
)
