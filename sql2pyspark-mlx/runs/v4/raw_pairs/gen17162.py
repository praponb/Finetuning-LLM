from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") != "Flash")
    .groupBy("status")
    .agg(F.max("cost").alias("max_cost"))
    .filter(F.col("max_cost") > 37)
)
