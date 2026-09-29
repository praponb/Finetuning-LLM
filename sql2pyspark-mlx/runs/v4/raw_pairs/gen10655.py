from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") >= 58000)
    .select(F.col("hire_date").alias("event_date"))
    .intersect(df_stores.select(F.col("opened_date").alias("event_date")))
    .orderBy("event_date")
)
