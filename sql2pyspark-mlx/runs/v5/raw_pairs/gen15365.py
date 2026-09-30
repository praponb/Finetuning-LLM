from pyspark.sql import functions as F

result = (
    df_payments
    .select("status")
    .intersect(df_shipments.select("status"))
)
