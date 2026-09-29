from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(
        F.sum(F.when(F.col("carrier") == "DHL", F.col("cost")).otherwise(0)).alias("dhl_cost"),
        F.sum(F.when(F.col("status") == "LOST", F.col("cost")).otherwise(0)).alias("lost_cost"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .orderBy("dhl_cost")
)
