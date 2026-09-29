from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("full_name").like("Kan%") & F.col("salary").between(90000, 141000))
    .orderBy("full_name")
)
