from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id")
w2 = Window.orderBy("is_remote_flag", "id")
w3 = Window.partitionBy("manager_id").orderBy(F.col("next_is_remote_flag").desc(), "id")
w4 = Window.partitionBy("level", "dept_id").orderBy(F.col("row_seq").asc_nulls_last())
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.avg("salary").alias("avg_employee_salary"))

cleaned = (
    df_employees
    .filter(F.col("hire_date").between("2024-03-04", "2024-04-01"))
    .select(
        "*",
        F.col("is_remote").cast("int").alias("is_remote_flag"),
        F.coalesce("level", F.lit("OTHER")).alias("level_filled"),
    )
)

enriched = (
    cleaned
    .filter(F.col("manager_id").isin([5, 15, 21]))
    .withColumn("prev_salary", F.lag("salary").over(w))
    .withColumn("next_is_remote_flag", F.lead("is_remote_flag").over(w))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("next_salary", F.lead("salary", 1, 0).over(w2))
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

ranked_employees = (
    ranked
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked_employees
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "id",
        "email",
        "dept_id",
        "manager_id",
        "hire_date",
        "level",
        "is_remote_flag",
        "level_filled",
        "prev_salary",
        F.col("avg_employee_salary"),
    )
)
