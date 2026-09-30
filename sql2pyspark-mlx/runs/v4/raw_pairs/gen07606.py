from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date")
    .unionByName(
        df_employees
        .filter(F.col("level") == "L3")
        .select(F.col("hire_date").alias("order_date"))
    )
    .orderBy(F.col("order_date").desc())
)
