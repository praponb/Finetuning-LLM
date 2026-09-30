from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["card", "promptpay", "bank_transfer"]))
    .select("status")
    .unionByName(df_shipments.filter(F.col("order_id").isNotNull()).select("status"))
    .orderBy("status")
)
