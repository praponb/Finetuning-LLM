from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("shipped_date").alias("event_date"))
    .unionByName(
        df_employees
        .filter(F.col("is_remote"))
        .select(F.col("hire_date").alias("event_date"))
    )
)
