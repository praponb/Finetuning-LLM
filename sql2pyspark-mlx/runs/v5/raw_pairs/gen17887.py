from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 27)
    .select("status")
    .intersect(df_shipments.select("status"))
    .orderBy(F.col("status").desc())
)
