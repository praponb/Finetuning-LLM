from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isNull() & F.col("status").isin(["SHIPPED", "PENDING", "COMPLETED"]))
    .select("id", "amount")
    .orderBy("id")
)
