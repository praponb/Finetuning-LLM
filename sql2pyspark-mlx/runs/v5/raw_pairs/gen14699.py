from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(80, 84))
    .groupBy("carrier")
    .pivot("status", ["RETURNED", "DELIVERED", "LOST", "IN_TRANSIT"])
    .agg(F.count("cost"))
)
