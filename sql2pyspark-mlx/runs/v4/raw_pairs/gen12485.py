from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("level", "job_title").orderBy("hire_date", "id")
w3 = Window.partitionBy("manager_id", "job_title").orderBy(F.col("hire_date").desc(), "id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.min("salary").alias("min_employee_salary"))

cleaned = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (F.col("hire_date") >= "2024-07-01"))
    .select(
        "id",
        "email",
        "manager_id",
        "job_title",
        "hire_date",
        "is_remote",
        "level",
        F.regexp_replace("full_name", " ", "_").alias("full_name_slug"),
    )
)

metrics = (
    cleaned
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("seq_no_2", F.row_number().over(w3))
)

joined = (
    metrics
    .alias("a")
    .join(df_employees.alias("p"), F.col("a.manager_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.job_title").alias("employee_job_title"),
        F.col("p.full_name"),
    )
)

result = (
    joined
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "manager_id",
        "is_remote",
        "level",
        "full_name_slug",
        "seq_no",
        "employee_job_title",
        "full_name",
        F.col("min_employee_salary"),
    )
)
