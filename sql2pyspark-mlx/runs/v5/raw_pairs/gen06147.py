from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") < "2023-12-17") | F.col("carrier").isin(["FedEx", "DHL", "Kerry"]))
    .select("id", "shipped_date", "order_id", F.round("cost", 0).alias("cost_rounded"))
    .orderBy(F.col("id").desc())
    .limit(50)
)
