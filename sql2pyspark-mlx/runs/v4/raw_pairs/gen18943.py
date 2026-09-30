from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") > "2025-08-01")
    .groupBy("carrier")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
    .orderBy("carrier", "avg_cost")
    .limit(1)
)
