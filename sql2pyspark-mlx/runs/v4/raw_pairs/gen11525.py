from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("full_name") > 7)
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
    .orderBy("salary_range")
)
