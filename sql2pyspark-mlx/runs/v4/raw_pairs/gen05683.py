from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~F.col("carrier").isin(["FedEx", "Flash"]) | ~F.col("carrier").isin(["DHL", "Kerry", "FedEx"]))
    .select("id", "cost", F.coalesce("cost", F.lit(0)).alias("cost_filled"))
)
