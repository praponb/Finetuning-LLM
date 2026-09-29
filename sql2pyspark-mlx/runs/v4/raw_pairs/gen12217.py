from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"),
        F.min("shipped_date").alias("first_shipped_date"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 3)
    .drop("cnt")
)
