from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 1190)
    .select("status")
    .intersect(df_shipments.select("status"))
    .orderBy("status")
)
