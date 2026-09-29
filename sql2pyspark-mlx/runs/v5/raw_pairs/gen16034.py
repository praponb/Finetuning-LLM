from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(15, 22))
    .select("discount")
)
