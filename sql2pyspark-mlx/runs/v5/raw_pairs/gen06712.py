from pyspark.sql import functions as F

result = (
    df_employees
    .select(F.lit("employee").alias("record_type"), "hire_date")
    .unionByName(
        df_shipments
        .filter(F.year("shipped_date") >= 2025)
        .select(
        F.lit("shipment").alias("record_type"),
        F.col("shipped_date").alias("hire_date"),
    )
    )
)
