from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.24, 0.25))
    .select("status", "channel")
    .distinct()
)
