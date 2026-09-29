from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status", F.date_trunc("MONTH", "delivered_date").alias("delivered_month"))
    .agg(F.min("cost").alias("min_cost"))
)
