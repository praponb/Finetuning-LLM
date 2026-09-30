from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 1240)
    .select("status")
    .unionByName(
        df_payments
        .filter(F.col("method").isin(["promptpay", "bank_transfer", "paypal"]))
        .select("status")
    )
    .distinct()
)
