from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id", "level").orderBy(F.col("hire_date").desc(), "id")
w2 = Window.orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("manager_id").orderBy(F.col("running_salary").desc(), "id")
w_ordered = Window.partitionBy("job_title", "location").orderBy("running_salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_name").orderBy("salary", "id")
w6 = Window.partitionBy("dept_id").orderBy("latest_rank", "id").rowsBetween(-5, -1)
w7 = Window.partitionBy("manager_id", "dept_name").orderBy(F.col("row_seq").asc_nulls_first(), "id")
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.avg("salary").alias("avg_employee_salary"))

metrics = (
    df_employees
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("running_salary", F.sum("salary").over(w2))
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .alias("e")
    .join(df_departments.alias("b"), F.col("e.dept_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.budget"), F.col("b.location"), F.col("b.dept_name"))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "left")
    .select(
        "src.*",
        F.col("d.dept_name").alias("department_dept_name"),
        F.col("d.location").alias("department_location"),
    )
)

enriched_employees = (
    with_dims
    .withColumn("cumulative_salary", F.sum("salary").over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("budget_avg_5_prior", F.avg("budget").over(w6))
    .withColumn("running_salary_2", F.sum("salary").over(w2))
)

latest_employees = (
    enriched_employees
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

base = (
    latest_employees
    .filter(F.col("full_name").like("Al%"))
    .select(
        "*",
        F.col("budget").cast("int").alias("budget_int"),
        (F.col("seq_no") * F.col("cumulative_salary")).alias("seq_no_times_cumulative_salary"),
    )
)

result = (
    base
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "manager_id",
        "salary",
        "hire_date",
        "level",
        "seq_no",
        "latest_rank",
        F.col("avg_employee_salary"),
    )
    .orderBy(F.col("avg_employee_salary").asc_nulls_first())
)
