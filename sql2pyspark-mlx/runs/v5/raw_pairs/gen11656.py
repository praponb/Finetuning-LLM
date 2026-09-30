from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id", "dept_name").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.partitionBy("job_title", "dept_name").orderBy("latest_rank", "id")
w3 = Window.partitionBy("job_title").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_id").orderBy("latest_rank", "id")
w5 = Window.orderBy("latest_rank", "id")
w6 = Window.partitionBy("dept_name").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("dept_name", "job_title").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("row_seq_2", "id").rowsBetween(-3, Window.currentRow)
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.max("salary").alias("max_employee_salary"))

cleaned = (
    df_employees
    .filter(F.col("job_title").isNotNull() & (~F.col("level").eqNullSafe(F.lit("L2"))))
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "hire_date",
        "level",
        F.lower("job_title").alias("job_title_lower"),
    )
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_departments.alias("b"), F.col("a.dept_id") == F.col("b.id"), "left")
    .filter(F.col("b.dept_name") != "Finance")
    .select(
        "a.id",
        "a.full_name",
        "a.dept_id",
        "a.job_title",
        "a.hire_date",
        F.col("b.dept_name"),
    )
)

ranked = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = ranked.select("*", F.row_number().over(w2).alias("row_seq"))

enriched_employees = (
    enriched
    .select(
        "*",
        F.sum("row_seq").over(w3).alias("cumulative_row_seq"),
        F.lead("row_seq", 2).over(w4).alias("next_row_seq"),
        F.row_number().over(w5).alias("row_seq_2"),
        F.last("job_title", ignorenulls=True).over(w6).alias("last_known_job_title"),
    )
)

with_windows = (
    enriched_employees
    .select(
        "id",
        "dept_id",
        "job_title",
        "row_seq",
        "row_seq_2",
        "last_known_job_title",
        F.sum("row_seq").over(w7).alias("cumulative_row_seq_2"),
        F.avg("next_row_seq").over(w8).alias("next_row_seq_avg_3"),
    )
)

by_type = (
    with_windows
    .groupBy("dept_id")
    .pivot("job_title", ["Manager", "Analyst"])
    .agg(F.max("row_seq_2"))
    .select(
        "dept_id",
        F.col("Manager").alias("manager"),
        F.col("Analyst").alias("analyst"),
    )
)

result = (
    by_type
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select("dept_id", "analyst", F.col("max_employee_salary"))
    .orderBy(F.col("analyst").desc_nulls_first())
)
