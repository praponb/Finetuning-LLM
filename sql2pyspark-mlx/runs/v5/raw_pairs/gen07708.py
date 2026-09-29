from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "IN_TRANSIT")
    .select(F.lit("shipment").alias("record_type"), "delivered_date")
    .union(
        df_employees
        .select(F.lit("employee").alias("record_type"), "hire_date")
    )
    .distinct()
    .orderBy("delivered_date")
)
