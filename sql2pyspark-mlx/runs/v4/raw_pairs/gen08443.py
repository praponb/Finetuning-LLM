from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "LOST", "DELIVERED"]))
    .withColumn("cost_int", F.col("cost").cast("int"))
    .withColumn("is_dhl", F.when(F.col("carrier") == "DHL", 1).otherwise(0))
)
