from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([6, 7, 9]))
    .withColumn("carrier_group", F.when(F.col("carrier").isin(["Kerry", "FedEx"]), "Group A").otherwise("Other"))
    .orderBy(F.col("carrier_group").desc())
)
