from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date", F.lit("shipment").alias("source"))
    .union(df_employees.select("hire_date", F.lit("employee").alias("source")))
)
