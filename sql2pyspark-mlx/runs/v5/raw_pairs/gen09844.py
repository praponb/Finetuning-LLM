from pyspark.sql import functions as F

result = (
    df_employees
    .select(F.col("hire_date").alias("event_date"))
    .unionByName(df_inventory.select(F.col("last_restock").alias("event_date")))
    .distinct()
)
