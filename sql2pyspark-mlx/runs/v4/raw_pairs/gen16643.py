from pyspark.sql import functions as F

base = (
    df_employees
    .filter(~F.col("job_title").isin(["Analyst", "Designer", "Intern"]) & F.upper("email").like("%ANY%"))
    .withColumn("is_l1", F.when(F.col("level") == "L1", 1).otherwise(0))
)

employees_stats = (
    base
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(
        F.sum(F.when(F.col("job_title") == "Engineer", 1).otherwise(0)).alias("engineer_count"),
        F.count("is_l1").alias("is_l1_count"),
    )
)

result = (
    employees_stats
    .orderBy(F.col("is_l1_count").desc(), "hire_year")
    .limit(10)
)
