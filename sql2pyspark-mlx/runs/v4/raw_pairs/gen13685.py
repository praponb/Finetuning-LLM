from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .pivot("status", ["RETURNED", "LOST"])
    .agg(F.sum("cost"))
    .orderBy("carrier")
)
