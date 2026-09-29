from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier", "order_id")
    .pivot("status", ["RETURNED", "IN_TRANSIT"])
    .agg(F.sum("cost").alias("total"))
)
