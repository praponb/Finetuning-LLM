from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("carrier_title", F.initcap(F.trim("carrier")))
    .orderBy("delivered_date", "id")
    .limit(10)
)
