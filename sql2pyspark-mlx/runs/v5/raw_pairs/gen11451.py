from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("email").like("%ENR%") | ~F.col("full_name").like("Iv%"))
    .select("job_title", F.date_sub("hire_date", 51).alias("hire_date_minus_51d"))
)
