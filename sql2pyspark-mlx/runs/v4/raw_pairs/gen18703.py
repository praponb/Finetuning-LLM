from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .groupBy("carrier")
    .pivot("status", ["DELIVERED", "RETURNED", "LOST"])
    .agg(F.max("cost"))
)
