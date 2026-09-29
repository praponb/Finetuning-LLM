from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote"))
    .select("hire_date", F.lit("employee").alias("kind"))
    .union(df_shipments.select("shipped_date", F.lit("shipment").alias("kind")))
    .distinct()
)
