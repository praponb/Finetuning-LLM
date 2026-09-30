from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id", "carrier", F.quarter("shipped_date").alias("shipped_quarter"))
    .agg(
        F.sum(F.when(F.col("carrier") == "DHL", 1).otherwise(0)).alias("dhl_count"),
        F.min("delivered_date").alias("min_delivered_date"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("max_cost") >= 45)
    .drop("max_cost")
    .orderBy(F.col("dhl_count").desc())
)
