from pyspark.sql import functions as F

base = df_employees.filter((F.col("level") == "L1") & F.col("manager_id").isNull())

hire_month_summary = (
    base
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(
        F.sum(F.when(F.col("job_title") == "Manager", F.col("salary")).otherwise(0)).alias("manager_salary"),
        F.sum("salary").alias("total_salary"),
    )
)

result = (
    hire_month_summary
    .filter(F.col("total_salary") >= 769000)
    .select("hire_month", "manager_salary")
)
