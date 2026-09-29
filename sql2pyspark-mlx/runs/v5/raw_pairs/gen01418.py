from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") <= 1070)
    .select("shipping_fee")
    .intersect(df_payments.filter(F.col("amount") < 870).select("amount"))
)
