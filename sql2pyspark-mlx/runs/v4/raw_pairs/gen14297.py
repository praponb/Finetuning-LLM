from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(420, 1270) & F.col("method").isin(["bank_transfer", "cash", "paypal"]))
)
