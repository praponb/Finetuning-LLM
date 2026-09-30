from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("hire_date", "id")
w2 = Window.partitionBy("level", "job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("dept_id", "job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("manager_id", "level").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("hire_date", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("level", "dept_id").orderBy("running_salary", "id")
w7 = Window.orderBy("employee_hire_date", "id").rowsBetween(-2, Window.currentRow)
w8 = Window.partitionBy("level", "dept_id").orderBy("running_salary", "id").rowsBetween(-2, -1)
w9 = Window.partitionBy("dept_id").orderBy(F.col("employee_salary").asc_nulls_last())
w10 = Window.partitionBy("manager_id")

enriched = (
    df_employees
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("first_known_job_title", F.first("job_title", ignorenulls=True).over(w2))
    .withColumn("running_salary", F.sum("salary").over(w3))
    .withColumn("last_known_job_title", F.last("job_title", ignorenulls=True).over(w4))
    .withColumn("salary_avg_6", F.avg("salary").over(w5))
)

joined = (
    enriched
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "left")
    .select(
        "a.*",
        F.col("e.email").alias("employee_email"),
        F.col("e.hire_date").alias("employee_hire_date"),
        F.col("e.salary").alias("employee_salary"),
    )
)

metrics = (
    joined
    .withColumn("seq_no", F.row_number().over(w6))
    .withColumn("employee_salary_avg_2", F.avg("employee_salary").over(w7))
    .withColumn("salary_avg_2_prior", F.avg("salary").over(w8))
    .withColumn("employee_salary_cume_dist", F.cume_dist().over(w9))
    .withColumn("manager_id_max_salary_avg_6", F.max("salary_avg_6").over(w10))
)

summary = (
    metrics
    .rollup(
        F.when(F.col("employee_salary_cume_dist") >= 0.85, "HIGH").otherwise("LOW").alias("employee_salary_cume_dist_tier"),
    )
    .agg(
        F.max("row_seq").alias("max_row_seq"),
        F.min("seq_no").alias("min_seq_no"),
        F.percentile_approx("employee_salary_avg_2", 0.95).alias("employee_salary_avg_2_p95"),
        F.count("*").alias("event_count"),
    )
)

result = (
    summary
    .select(
        "employee_salary_cume_dist_tier",
        "max_row_seq",
        "employee_salary_avg_2_p95",
        "event_count",
    )
)
