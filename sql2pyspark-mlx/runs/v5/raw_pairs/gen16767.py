from pyspark.sql import functions as F

result = (
    df_employees
    .select(F.lit("employee").alias("record_type"), "hire_date")
    .union(
        df_shipments
        .filter(~F.col("status").isin(["IN_TRANSIT", "LOST"]))
        .select(F.lit("shipment").alias("record_type"), "shipped_date")
    )
    .distinct()
    .orderBy("hire_date")
)
