from pyspark.sql import functions as F

result = (
    df_employees
    .agg((F.max("salary") - F.min("salary")).alias("salary_range"))
    .orderBy(F.col("salary_range").desc())
)
