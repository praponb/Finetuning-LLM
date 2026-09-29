from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "Flash")
    .withColumn("carrier_title", F.initcap(F.trim("carrier")))
    .orderBy("id")
    .limit(20)
)
