from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 40)
    .select("order_id")
    .unionByName(df_payments.filter(F.col("order_id").isin([9, 24])).select("order_id"))
)
