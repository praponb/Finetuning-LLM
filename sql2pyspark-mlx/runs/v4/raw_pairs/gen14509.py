from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull() | F.col("amount").between(1310, 1320))
    .select("discount", "amount", "customer_id")
    .orderBy(F.col("customer_id").desc())
)
