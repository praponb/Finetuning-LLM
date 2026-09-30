from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED"]))
    .select("id", "status")
    .unionByName(
        df_payments
        .filter(F.col("method").isin(["paypal", "cash", "bank_transfer"]))
        .select("id", "status")
    )
)
