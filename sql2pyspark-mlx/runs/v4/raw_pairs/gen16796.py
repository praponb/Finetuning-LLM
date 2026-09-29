from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["DELIVERED", "IN_TRANSIT", "LOST"]) & F.col("carrier").isin(["FedEx", "UPS", "Flash"]))
    .withColumn("carrier_status", F.concat("carrier", F.lit(" "), "status"))
)
