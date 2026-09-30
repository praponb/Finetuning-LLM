from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([15, 23]) & (F.col("discount").isNull() | F.col("order_date").between("2024-03-01", "2025-06-02")))
    .select("id", "amount", "channel")
    .orderBy("channel")
)
