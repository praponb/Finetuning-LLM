from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .pivot("status", ["RETURNED", "IN_TRANSIT", "DELIVERED"])
    .agg(F.sum("cost"))
    .select(
        "carrier",
        F.col("RETURNED").alias("returned"),
        F.col("IN_TRANSIT").alias("in_transit"),
        F.col("DELIVERED").alias("delivered"),
    )
)
