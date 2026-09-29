from pyspark.sql import functions as F

result = (
    df_payments
    .select("method", "status")
    .distinct()
    .orderBy("status", "method")
    .limit(1)
)
