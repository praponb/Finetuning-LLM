from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["card", "paypal"]))
    .select("status")
    .unionByName(df_shipments.select("status"))
)
