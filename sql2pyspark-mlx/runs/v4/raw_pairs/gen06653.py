from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") < "2025-10-01")
    .select(F.col("hire_date").alias("event_date"))
    .intersect(df_inventory.select(F.col("last_restock").alias("event_date")))
)
