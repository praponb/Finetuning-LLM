from pyspark.sql import functions as F

result = (
    df_orders
    .select(F.col("order_date").alias("event_date"), F.lit("order").alias("source"))
    .unionByName(
        df_employees
        .filter(F.col("is_remote"))
        .select(F.col("hire_date").alias("event_date"), F.lit("employee").alias("source"))
    )
    .distinct()
)
