from pyspark.sql import functions as F

result = (
    df_accounts
    .select("status")
    .subtract(df_shipments.filter(F.col("order_id") == 38).select("status"))
    .orderBy(F.col("status").desc())
)
