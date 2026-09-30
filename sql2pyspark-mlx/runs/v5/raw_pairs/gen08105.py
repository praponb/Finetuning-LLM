from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        F.col("shipped_date").alias("event_date"),
        F.lit("shipment").alias("source"),
    )
    .unionByName(
        df_employees
        .filter(F.col("full_name").like("Al%"))
        .select(F.col("hire_date").alias("event_date"), F.lit("employee").alias("source"))
    )
    .distinct()
    .orderBy("source")
)
