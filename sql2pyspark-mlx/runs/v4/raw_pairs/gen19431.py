from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.year("delivered_date") == 2023) | (F.col("cost") <= 30))
    .withColumn("carrier_prefix", F.substring("carrier", 1, 4))
    .withColumn("adjusted_cost", F.round((F.col("cost") * 1.1), 2))
    .orderBy(F.col("cost").desc(), "carrier_prefix")
)
