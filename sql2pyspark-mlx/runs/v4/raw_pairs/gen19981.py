from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .pivot("status", ["IN_TRANSIT", "DELIVERED", "LOST", "RETURNED"])
    .agg(F.count("cost"))
    .orderBy("carrier")
)
