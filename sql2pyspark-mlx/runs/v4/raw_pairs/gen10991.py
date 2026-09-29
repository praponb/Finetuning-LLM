from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level")
w2 = Window.orderBy(F.col("hire_date").desc(), "id")
w3 = Window.partitionBy("dept_id").orderBy("salary", "id")
w4 = Window.partitionBy("job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("dept_id").orderBy("row_seq", "id").rowsBetween(-5, -1)

with_windows = (
    df_employees
    .withColumn("salary_share", F.round((F.col("salary") / F.sum("salary").over(w)), 4))
    .withColumn("row_seq", F.row_number().over(w2))
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_dims = (
    latest
    .alias("s")
    .join(df_employees.alias("b"), F.col("s.manager_id") == F.col("b.id"), "left")
    .select(
        "s.*",
        F.col("b.email").alias("employee_email"),
        F.col("b.job_title").alias("employee_job_title"),
    )
)

metrics = (
    with_dims
    .withColumn("cumulative_salary", F.sum("salary").over(w4))
    .withColumn("salary_share_avg_5_prior", F.avg("salary_share").over(w5))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("cumulative_salary") >= 602146, "HIGH").otherwise("LOW").alias("cumulative_salary_tier"),
        F.date_trunc("QUARTER", "hire_date").alias("quarter_start"),
        "employee_job_title",
    )
    .agg(
        F.max("salary_share").alias("max_salary_share"),
        F.count_if(F.col("job_title") == "Manager").alias("manager_events"),
        F.sum(F.when(F.col("employee_job_title") == "Intern", F.col("salary_share_avg_5_prior")).otherwise(0)).alias("intern_salary_share_avg_5_prior"),
        F.sum(F.when(F.col("employee_job_title") == "Engineer", F.col("salary_share_avg_5_prior")).otherwise(0)).alias("engineer_salary_share_avg_5_prior"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
    )
    .filter(F.col("intern_salary_share_avg_5_prior") > 0)
    .orderBy(F.col("avg_row_seq").desc_nulls_last())
)
