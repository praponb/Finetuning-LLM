from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") <= 146000)
    .agg(F.max("hire_date").alias("latest_hire_date"))
    .orderBy("latest_hire_date")
)
