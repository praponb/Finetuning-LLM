from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary")
w2 = Window.partitionBy("dept_id").orderBy(F.col("hire_date").desc(), "id")
w3 = Window.partitionBy("dept_id", "level").orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy("manager_id", "job_title").orderBy(F.col("salary").desc())
w5 = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")
w6 = Window.partitionBy("employee_level").orderBy("latest_rank", "id")
w7 = Window.partitionBy("manager_id")
w8 = Window.partitionBy("job_title")
w9 = Window.partitionBy("level").orderBy(F.col("salary_rank").desc(), "id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.sum("salary").alias("sum_employee_salary"))

metrics = (
    df_employees
    .select(
        "*",
        F.cume_dist().over(w).alias("salary_cume_dist"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("row_seq"),
        F.rank().over(w4).alias("salary_rank"),
    )
)

enriched = (
    metrics
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "left")
    .select("a.*", F.col("e.level").alias("employee_level"))
)

deduped = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

valid = (
    deduped
    .filter((F.col("hire_date") >= F.lit("2023-09-28").cast("date")) & F.col("is_remote"))
    .withColumn("job_title_title", F.initcap(F.trim("job_title")))
    .withColumn("clean_email", F.trim("email"))
)

deduped_employees = (
    valid
    .withColumn("latest_rank_2", F.row_number().over(w6))
    .filter(F.col("latest_rank_2") == 1)
)

enriched_employees = (
    deduped_employees
    .withColumn("event_seq_share", F.round((F.col("event_seq") / F.sum("event_seq").over(w7)), 4))
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w8)), 4))
    .withColumn("salary_rank_bucket", F.ntile(10).over(w9))
)

result = (
    enriched_employees
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "job_title",
        "is_remote",
        "salary_cume_dist",
        "event_seq",
        "row_seq",
        F.col("sum_employee_salary"),
    )
    .orderBy(F.col("row_seq").desc())
)
