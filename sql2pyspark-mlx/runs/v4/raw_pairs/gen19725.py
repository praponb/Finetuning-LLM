from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") < "2025-09-21")
    .select("delivered_date")
    .union(df_employees.select("hire_date"))
)
