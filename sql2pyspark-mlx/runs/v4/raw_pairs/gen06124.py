from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([1, 10, 11]))
    .groupBy(F.quarter("delivered_date").alias("delivered_quarter"))
    .agg(F.max("cost").alias("max_cost"))
)
