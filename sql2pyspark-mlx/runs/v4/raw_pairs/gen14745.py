from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "partner")
    .select(F.col("order_date").alias("event_date"))
    .unionByName(
        df_employees
        .filter(F.col("is_remote"))
        .select(F.col("hire_date").alias("event_date"))
    )
    .distinct()
    .orderBy(F.col("event_date").desc())
)
