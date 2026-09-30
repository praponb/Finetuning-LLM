from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .pivot("status", ["DELIVERED", "RETURNED", "IN_TRANSIT"])
    .agg(F.sum("cost"))
    .orderBy("carrier")
)
