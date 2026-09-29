from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .intersect(df_shipments.filter(F.col("cost").between(70, 96)).select("status"))
)
