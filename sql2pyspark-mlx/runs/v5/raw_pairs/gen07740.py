from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date") <= "2024-09-13")
    .select("hire_date", F.lit("employee").alias("source"))
    .union(
        df_orders
        .filter(F.col("discount").between(0.21, 0.22))
        .select("order_date", F.lit("order").alias("source"))
    )
    .distinct()
)
