from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("level") == "L3") | (F.col("salary") > 74000))
    .groupBy("job_title")
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
    .filter(F.col("salary_range") >= 38)
)
