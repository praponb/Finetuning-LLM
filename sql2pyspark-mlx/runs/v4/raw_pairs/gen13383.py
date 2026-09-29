from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date", F.lit("store").alias("source"))
    .union(df_employees.select("hire_date", F.lit("employee").alias("source")))
    .distinct()
)
