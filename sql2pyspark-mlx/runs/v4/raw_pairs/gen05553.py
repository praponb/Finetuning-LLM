from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([2, 17, 38]))
    .select("method", "status")
    .unionByName(df_payments.filter(F.col("order_id") == 31).select("method", "status"))
    .distinct()
    .orderBy("status")
)
