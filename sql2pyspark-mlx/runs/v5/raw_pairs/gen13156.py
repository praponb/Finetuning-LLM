from pyspark.sql import functions as F

result = (
    df_payments
    .select("method")
    .distinct()
    .crossJoin(df_subscriptions.select("status").distinct())
    .orderBy("method", "status")
)
