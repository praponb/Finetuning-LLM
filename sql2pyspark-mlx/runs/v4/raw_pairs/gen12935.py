from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").between(0.19, 0.34))
    .select("status", "discount")
)
