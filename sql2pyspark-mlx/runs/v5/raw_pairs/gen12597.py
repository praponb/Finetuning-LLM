from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "dept_id").orderBy(F.col("hire_date_minus_51d").desc(), "id")
w2 = Window.partitionBy("manager_id").orderBy("latest_rank", "id")
w3 = Window.partitionBy("manager_id").orderBy("salary", "id")
w4 = Window.partitionBy("manager_id")
w_ordered = Window.partitionBy("job_title").orderBy("salary_bucket", "id")
w6 = Window.partitionBy("job_title")
w_recent = Window.partitionBy("dept_id", "job_title").orderBy("prev_salary", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.orderBy("hire_date_minus_51d", "id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.max("salary").alias("max_employee_salary"))

base = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-06-28").cast("date")) & F.col("level").isNotNull())
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.date_sub("hire_date", 51).alias("hire_date_minus_51d"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    ranked
    .withColumn("prev_salary", F.lag("salary", 2).over(w2))
    .withColumn("salary_bucket", F.ntile(3).over(w3))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w4)), 4))
)

with_windows = (
    metrics
    .filter(F.col("is_remote"))
    .select(
        "*",
        F.ntile(10).over(w_ordered).alias("salary_bucket_bucket"),
        F.count("prev_salary").over(w6).alias("job_title_count_prev_salary"),
        F.avg("prev_salary").over(w_recent).alias("prev_salary_avg_6"),
        F.row_number().over(w8).alias("event_seq"),
    )
)

enriched = (
    with_windows
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.dept_name"))
)

result = (
    enriched
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "email",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        "hire_date_minus_51d",
        "latest_rank",
        "salary_bucket",
        F.col("max_employee_salary"),
    )
    .orderBy(F.col("level").desc())
)
