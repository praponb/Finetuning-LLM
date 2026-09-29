from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["DELIVERED", "LOST", "IN_TRANSIT"]))
    .withColumn("cost_whole", F.floor("cost"))
    .orderBy("status", "id")
    .limit(10)
)
