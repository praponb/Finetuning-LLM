from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~(F.col("status").isin(["REFUNDED", "SUCCESS"]) | (F.col("method") != "card")))
    .select("amount", "method", "status")
)
