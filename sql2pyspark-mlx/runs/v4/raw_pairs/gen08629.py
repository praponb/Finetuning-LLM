from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status", "carrier")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
    .orderBy("carrier")
)
