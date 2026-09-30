from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.to_date("paid_at") >= "2024-06-01")
    .select("status")
    .unionByName(df_shipments.select("status"))
    .distinct()
    .orderBy("status")
)
