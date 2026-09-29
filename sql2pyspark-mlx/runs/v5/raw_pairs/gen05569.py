from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(33, 87))
    .groupBy("carrier")
    .pivot("status", ["LOST", "IN_TRANSIT", "DELIVERED"])
    .agg(F.count("cost"))
    .orderBy("carrier")
)
