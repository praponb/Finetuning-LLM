from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(630, 1100))
    .select("status")
    .distinct()
)
