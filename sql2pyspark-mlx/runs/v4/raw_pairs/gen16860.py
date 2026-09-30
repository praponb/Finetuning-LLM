from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy(F.col("employee_hire_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("employee_job_title", "dept_id").orderBy(F.col("employee_salary").desc_nulls_first(), "id")
w3 = Window.partitionBy("job_title", "employee_job_title").orderBy(F.col("salary").desc(), "id")
w4 = Window.partitionBy("dept_id").orderBy("salary")
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.sum("salary").alias("sum_employee_salary"))

enriched = (
    df_employees
    .alias("e")
    .join(df_employees.alias("d"), F.col("e.manager_id") == F.col("d.id"), "left")
    .select(
        "e.*",
        F.col("d.job_title").alias("employee_job_title"),
        F.col("d.salary").alias("employee_salary"),
        F.col("d.hire_date").alias("employee_hire_date"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

deduped = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

enriched_employees = (
    deduped
    .alias("e")
    .join(df_departments.alias("p"), F.col("e.dept_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.location"), F.col("p.dept_name"), F.col("p.budget"))
)

latest = (
    enriched_employees
    .withColumn("dedup_rank_2", F.row_number().over(w3))
    .filter(F.col("dedup_rank_2") == 1)
    .drop("dedup_rank_2")
)

ranked_employees = (
    latest
    .withColumn("rn", F.rank().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

filtered = (
    ranked_employees
    .filter(F.col("manager_id").isNotNull())
    .withColumn("email_level", F.concat("email", F.lit(", "), "level"))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("N/A")))
)

result = (
    filtered
    .join(salary_by_dept_id, on="dept_id", how="left")
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
        F.col("sum_employee_salary"),
    )
)
