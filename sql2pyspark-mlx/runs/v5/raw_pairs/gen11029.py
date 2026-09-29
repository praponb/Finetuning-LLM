from pyspark.sql import functions as F

src = (
    df_employees
    .filter(F.col("full_name").like("%Henr%") & ~F.col("is_remote"))
    .withColumn("email_filled", F.coalesce("email", F.lit("Unknown")))
)

employees_stats = (
    src
    .groupBy("level", F.date_trunc("MONTH", "hire_date").alias("hire_month"))
    .agg(
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_count"),
        F.sum("salary").alias("total_salary"),
    )
)

result = (
    employees_stats
    .filter(F.col("manager_count") > 3)
    .select("level", "hire_month", "manager_count")
    .orderBy("hire_month", "level")
    .limit(5)
)
