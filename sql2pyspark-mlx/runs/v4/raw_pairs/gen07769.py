from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("manager_id").isin([21, 26, 29, 37]) & F.upper("full_name").like("%MMA%"))
    .withColumn("level_upper", F.upper("level"))
)

agg = (
    base
    .groupBy(F.year("hire_date").alias("hire_year"))
    .agg(F.count(F.when(F.col("job_title") == "Engineer", 1)).alias("engineer_count"))
)

result = agg.orderBy(F.col("hire_year").desc())
