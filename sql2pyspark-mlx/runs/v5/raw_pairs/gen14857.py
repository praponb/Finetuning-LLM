from pyspark.sql import functions as F

result = (
    df_shipments
    .select(F.col("delivered_date").alias("event_date"))
    .intersect(
        df_employees
        .filter(F.col("email").isNotNull())
        .select(F.col("hire_date").alias("event_date"))
    )
    .orderBy("event_date")
)
