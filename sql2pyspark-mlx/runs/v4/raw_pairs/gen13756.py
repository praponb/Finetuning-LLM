from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["cash", "card", "promptpay"]))
    .select("id", "status", "method")
    .orderBy("status", F.col("id").desc())
)
