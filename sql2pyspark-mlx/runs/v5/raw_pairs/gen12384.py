from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.col("delivered_date") < "2024-03-03")

agg = (
    recent_shipments
    .groupBy("carrier")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)

result = (
    agg
    .filter(F.col("stddev_cost") > 76)
    .select("carrier", "stddev_cost")
)
