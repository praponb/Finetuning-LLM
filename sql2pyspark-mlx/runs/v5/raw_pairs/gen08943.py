from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("salary_scaled").desc(), "id")
w2 = Window.partitionBy("dept_id", "manager_id").orderBy(F.col("hire_date").desc(), "id")
w3 = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("manager_id").orderBy("rn", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("manager_id")
w6 = Window.partitionBy("dept_id")
w7 = Window.partitionBy("manager_id", "dept_id").orderBy(F.col("salary_scaled_avg_3").desc())
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.sum("salary").alias("sum_employee_salary"))

filtered = (
    df_employees
    .filter(F.col("hire_date") > F.lit("2023-04-03").cast("date"))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        F.round((F.col("salary") * 0.85), 2).alias("salary_scaled"),
        F.coalesce("job_title", F.lit("N/A")).alias("resolved_job_title"),
    )
)

latest = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    latest
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "resolved_job_title",
        "rn",
        F.row_number().over(w2).alias("event_seq"),
        F.sum("salary_scaled").over(w3).alias("running_salary_scaled"),
        F.avg("salary_scaled").over(w4).alias("salary_scaled_avg_3"),
        F.min("salary").over(w5).alias("manager_id_min_salary"),
        F.round((F.col("salary") / F.sum("salary").over(w6)), 4).alias("salary_share"),
    )
)

ranked = (
    scored
    .withColumn("dedup_rank", F.rank().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    ranked
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "dept_id",
        "manager_id",
        "resolved_job_title",
        "event_seq",
        "running_salary_scaled",
        "salary_scaled_avg_3",
        F.when(F.col("full_name").rlike("^(Emma Brown|Ivy Chen)$"), "GROUP_A").when(F.col("full_name").rlike("^(Alice Wong|bob smith)$"), "OTHER_ZONE").otherwise("CORE").alias("full_name_zone"),
        F.col("sum_employee_salary"),
    )
    .orderBy(F.col("event_seq").asc_nulls_first())
)
