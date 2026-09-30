from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .select("status")
    .intersect(df_shipments.select("status"))
    .orderBy(F.col("status").desc())
)
