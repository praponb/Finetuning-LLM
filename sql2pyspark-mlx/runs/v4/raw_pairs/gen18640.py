from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["bank_transfer", "promptpay"]))
    .select("amount")
    .union(df_store_sales.select("revenue"))
    .distinct()
)
