from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "Flash")
    .groupBy("carrier")
    .pivot("status", ["RETURNED", "DELIVERED", "IN_TRANSIT", "LOST"])
    .agg(F.max("cost"))
)
