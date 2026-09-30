from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.when(F.col("cost") >= 108, "high").when(F.col("cost") >= 30, "mid").otherwise("low").alias("cost_bucket"))
    .pivot("carrier", ["FedEx", "UPS"])
    .agg(F.sum("cost"))
    .orderBy("cost_bucket")
)
