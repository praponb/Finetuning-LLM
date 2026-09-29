from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "DHL"]))
    .groupBy("status")
    .agg(F.sum("cost").alias("total_cost"))
    .filter(F.col("total_cost") > 490)
    .select("status", "total_cost")
    .orderBy("total_cost")
)
