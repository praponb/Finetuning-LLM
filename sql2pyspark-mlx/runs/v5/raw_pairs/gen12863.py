from pyspark.sql import functions as F

filtered_employees = df_employees.filter(~F.col("job_title").isin(["Analyst", "Manager"]))

summary = (
    filtered_employees
    .groupBy(
        F.when(F.year("hire_date") < 2024, "before_2024").when(F.year("hire_date") == 2024, "2024").otherwise("after_2024").alias("hire_cohort"),
    )
    .agg(F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"))
)

result = (
    summary
    .select("hire_cohort", "engineer_count")
    .orderBy("engineer_count")
)
