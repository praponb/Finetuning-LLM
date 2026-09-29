from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .subtract(df_payments.filter(F.col("amount") < 1220).select("status"))
    .orderBy("status")
)
