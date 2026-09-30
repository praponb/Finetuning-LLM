from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["cash", "card", "bank_transfer"]))
    .select("status")
    .distinct()
    .orderBy("status")
)
