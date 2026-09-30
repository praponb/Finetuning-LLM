from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.max("cost").alias("max_cost"),
        F.avg("cost").alias("avg_cost"),
        F.sum(F.when(F.col("carrier") == "FedEx", F.col("cost")).otherwise(0)).alias("fedex_cost"),
    )
    .filter(F.col("fedex_cost") >= 76)
    .drop("fedex_cost")
)
