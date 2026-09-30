from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(10, 18))
    .select("id", "amount")
)
