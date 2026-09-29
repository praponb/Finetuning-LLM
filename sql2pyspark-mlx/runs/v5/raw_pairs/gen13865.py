from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("cost_band", F.when(F.col("cost") >= 95, "High").when(F.col("cost") >= 42, "Medium").otherwise("Low"))
    .withColumn("status_carrier", F.concat("status", F.lit(" - "), "carrier"))
    .orderBy("id")
)
