from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([2, 6, 12]) & (F.col("carrier").isin(["FedEx", "Kerry", "DHL"]) | F.col("order_id").isin([7, 35])))
    .orderBy(F.col("delivered_date").desc(), "carrier")
)
