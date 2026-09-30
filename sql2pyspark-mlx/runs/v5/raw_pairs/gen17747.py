from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(760, 1500) & (F.col("order_id").isin([10, 15, 23]) | F.col("order_id").isNull()))
    .select("id", "method", "order_id")
    .orderBy("method")
)
