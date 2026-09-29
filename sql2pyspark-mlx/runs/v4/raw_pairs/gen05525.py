from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(29, 96))
    .groupBy(F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"))
    .agg(F.max("cost").alias("max_cost"))
    .orderBy("max_cost", "delivered_month")
    .limit(10)
)
