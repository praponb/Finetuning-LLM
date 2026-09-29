from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .select("status")
    .distinct()
    .orderBy("status")
)
