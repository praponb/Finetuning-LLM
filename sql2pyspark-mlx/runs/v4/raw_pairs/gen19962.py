from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 860)
    .select("method")
    .subtract(
        df_payments
        .filter(F.col("method") != "bank_transfer")
        .select("method")
    )
)
