from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L4", "L2"]))
    .select("hire_date")
    .unionByName(df_orders.select(F.col("order_date").alias("hire_date")))
)
