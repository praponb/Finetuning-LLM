from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(-6, Window.currentRow)
w_hist = Window.partitionBy("dept_id", "level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy("salary")
w4 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist_2 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc(), "id")
w6 = Window.partitionBy("manager_id")
w7 = Window.orderBy(F.col("cumulative_salary").desc())
w_running = Window.partitionBy("level", "dept_id").orderBy(F.col("cumulative_salary").desc_nulls_first())
salary_by_manager_id = df_employees.groupBy("manager_id").agg(F.sum("salary").alias("sum_employee_salary"))

metrics = (
    df_employees
    .select(
        "*",
        F.avg("salary").over(w).alias("salary_avg_6"),
        F.sum("salary").over(w_hist).alias("running_salary"),
        F.percent_rank().over(w3).alias("salary_percent_rank"),
        F.sum("salary").over(w4).alias("cumulative_salary"),
        F.row_number().over(w_hist_2).alias("row_seq"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_employees.alias("b"), F.col("s.manager_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.level").alias("employee_level"))
)

remaining_keys = (
    enriched
    .select("dept_id", "job_title")
    .subtract(
        df_employees
        .filter(F.col("job_title").isin(["Manager", "Analyst", "Director"]))
        .select("dept_id", "job_title")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.dept_id") == F.col("k.dept_id")) & (F.col("s.job_title") == F.col("k.job_title")), "inner")
    .select("s.*")
)

enriched_employees = (
    verified
    .withColumn("manager_id_min_running_salary", F.min("running_salary").over(w6))
    .withColumn("cumulative_salary_cume_dist", F.cume_dist().over(w7))
    .withColumn("cumulative_salary_rank", F.rank().over(w_running))
)

result = (
    enriched_employees
    .join(salary_by_manager_id, on="manager_id", how="left")
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "salary",
        "hire_date",
        "is_remote",
        "level",
        F.col("sum_employee_salary"),
    )
)
