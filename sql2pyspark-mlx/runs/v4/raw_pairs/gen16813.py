from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .withColumn("cost_tier", F.when(F.col("cost") >= 97, "High").when(F.col("cost") >= 40, "Medium").otherwise("Low"))
    .withColumn("cost_filled", F.coalesce("cost", F.lit(0)))
)
