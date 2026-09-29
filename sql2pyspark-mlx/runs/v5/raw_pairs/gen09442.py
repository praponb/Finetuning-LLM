from pyspark.sql import functions as F

result = (
    df_shipments
    .select("delivered_date")
    .union(df_employees.select("hire_date"))
    .distinct()
    .orderBy(F.col("delivered_date").desc())
)
