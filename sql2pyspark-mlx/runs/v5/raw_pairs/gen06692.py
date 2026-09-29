from pyspark.sql import functions as F

result = (
    df_payments
    .select("status")
    .unionByName(df_shipments.filter(F.col("cost") <= 95).select("status"))
)
