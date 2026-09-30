from pyspark.sql import functions as F

filtered_employees = df_employees.filter(F.col("manager_id").isin([8, 20, 22, 26]))

summary = (
    filtered_employees
    .groupBy("level")
    .agg(
        F.max("salary").alias("max_salary"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0))) / F.count("*")), 1).alias("pct_manager"),
    )
)

result = (
    summary
    .select("level", "max_salary", "pct_manager")
    .orderBy(F.col("pct_manager").desc(), "level")
    .limit(3)
)
