from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "DHL", "Kerry"]))
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("carrier") == "UPS", F.col("cost")).otherwise(0)).alias("ups_cost"),
    )
    .filter(F.col("ups_cost") >= 36)
)
