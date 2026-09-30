from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select("status")
    .subtract(df_payments.select("status"))
    .orderBy("status")
)
