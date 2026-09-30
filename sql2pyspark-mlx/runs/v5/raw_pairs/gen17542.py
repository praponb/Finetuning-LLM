from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date", F.lit("employee").alias("record_type"))
    .unionByName(
        df_orders
        .select(F.col("order_date").alias("hire_date"), F.lit("order").alias("record_type"))
    )
    .distinct()
)
