from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
    .filter(F.col("cost_range") < 24)
    .orderBy("carrier", F.col("cost_range").desc())
    .limit(50)
)
