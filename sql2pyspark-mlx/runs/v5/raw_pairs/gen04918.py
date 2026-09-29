from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .pivot("status", ["RETURNED", "DELIVERED", "LOST"])
    .agg(F.count("cost"))
    .orderBy("carrier")
)
