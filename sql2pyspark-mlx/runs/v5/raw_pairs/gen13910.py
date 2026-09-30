from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["FedEx", "Kerry"]) & F.col("status").isin(["RETURNED", "DELIVERED", "IN_TRANSIT"]) & (F.col("carrier") == "Kerry"))
)
