from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg((F.max("cost") - F.min("cost")).alias("cost_range"))
    .filter(F.col("cost_range") < 37)
    .select("carrier", "cost_range")
    .orderBy(F.col("carrier").desc(), "cost_range")
    .limit(3)
)
