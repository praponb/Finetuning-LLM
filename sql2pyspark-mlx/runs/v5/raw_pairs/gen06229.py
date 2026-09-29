from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .pivot("status", ["DELIVERED", "IN_TRANSIT"])
    .agg(F.avg("cost"))
    .orderBy("carrier")
)
