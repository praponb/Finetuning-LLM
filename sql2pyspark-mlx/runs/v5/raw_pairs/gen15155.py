from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date", F.lit("employee").alias("origin"))
    .union(df_stores.select("opened_date", F.lit("store").alias("origin")))
    .distinct()
)
