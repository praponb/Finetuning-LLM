from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date", F.lit("employee").alias("source"))
    .union(
        df_shipments
        .select("delivered_date", F.lit("shipment").alias("source"))
    )
    .distinct()
)
