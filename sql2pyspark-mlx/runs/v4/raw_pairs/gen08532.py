from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2023-06-01")
    .select("status")
    .unionByName(df_shipments.select("status"))
    .distinct()
)
