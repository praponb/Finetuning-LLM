from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 9)
    .select(F.lit("shipment").alias("origin"), "shipped_date")
    .union(
        df_employees
        .filter(F.col("job_title").isNotNull())
        .select(F.lit("employee").alias("origin"), "hire_date")
    )
    .distinct()
)
