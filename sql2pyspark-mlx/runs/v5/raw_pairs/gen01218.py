from pyspark.sql import functions as F

result = (
    df_orders
    .select("channel")
    .distinct()
    .crossJoin(df_payments.select("method").distinct())
    .orderBy("channel", "method")
)
