from pyspark.sql import functions as F

src = (
    df_employees
    .filter(F.col("dept_id").isin([4, 9, 13, 21]) & (F.col("job_title") != "Manager"))
)

agg = (
    src
    .groupBy("job_title")
    .agg(F.max("hire_date").alias("last_hire_date"))
)

result = agg.orderBy("last_hire_date")
