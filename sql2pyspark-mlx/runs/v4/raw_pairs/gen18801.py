from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["DHL", "FedEx"]))
    .groupBy("carrier")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.max("delivered_date").alias("last_delivered_date"),
        F.avg("cost").alias("avg_cost"),
    )
    .filter(F.col("avg_cost") < 60)
)
