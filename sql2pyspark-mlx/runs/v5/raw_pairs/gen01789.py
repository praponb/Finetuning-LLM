from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([26, 30, 35]) & F.col("store_id").isin([4, 15, 18, 23]))
    .select("discount", "status")
    .orderBy(F.col("discount").desc())
)
